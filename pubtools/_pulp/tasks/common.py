import os
import logging
import datetime
import attr

from more_executors.futures import f_map, f_flat_map, f_sequence

from pushsource import FilePushItem, ModuleMdPushItem, RpmPushItem
from pubtools.pulplib import (
    PublishOptions,
    FileUnit,
    ModulemdUnit,
    PublishOptions,
    RpmUnit,
)

from pubtools._pulp.task import PulpTask
from pubtools._pulp.services import (
    FastPurgeClientService,
    UdCacheClientService,
    CollectorService,
)

from ..hooks import pm

LOG = logging.getLogger("pubtools.pulp")

step = PulpTask.step

# Since these classes are designed to be mixed in with PulpTask but pylint
# doesn't know that...
# pylint: disable=no-member


class CDNCache(FastPurgeClientService):
    """Provide features to interact with CDN cache."""

    @step("Flush CDN cache")
    def flush_cdn(self, repos):
        """Clears the CDN cache for the repositories provided

        Arguments:
            repos (list)
                Repositories to be cleared from the cache

        Returns:
            list[Future]
                List of futures that resolve to the repository
                objects on completion
                An empty list when a client is not available
        """
        if not self.fastpurge_client:
            LOG.info("CDN cache flush is not enabled.")
            return []

        def purge_repo(repo):
            to_flush = []
            for url in repo.mutable_urls:
                flush_url = os.path.join(
                    self.fastpurge_root_url, repo.relative_url, url
                )
                to_flush.append(flush_url)

            LOG.debug("Flush: %s", to_flush)
            flush = self.fastpurge_client.purge_by_url(to_flush)
            return f_map(flush, lambda _: repo)

        return [purge_repo(r) for r in repos if r.relative_url]


class UdCache(UdCacheClientService):
    """Provide features to interact with UD cache."""

    @step("Flush UD cache")
    def flush_ud(self, repos):
        client = self.udcache_client
        if not client:
            LOG.info("UD cache flush is not enabled.")
            return []

        out = []
        for repo in repos:
            out.append(client.flush_repo(repo.id))

        return out


class Publisher(CDNCache, UdCache):
    """Provides behavior relating to Pulp repo publish which can be shared by
    multiple tasks."""

    def add_publisher_args(self, parser):
        group = parser.add_argument_group(
            "Publish options", "Options affecting the behavior of Pulp repo publishes."
        )
        group.add_argument(
            "--clean",
            help="attempt to delete remote content not in the repo",
            action="store_true",
        )
        group.add_argument(
            "--force",
            help="force publish of repos even if Pulp thinks nothing has changed",
            action="store_true",
        )

    @step("Publish")
    def publish(self, repos):
        out = []

        publish_opts = PublishOptions(force=self.args.force, clean=self.args.clean)
        for repo in repos:
            LOG.info("Publishing %s", repo.id)
            f = repo.publish(publish_opts)
            out.append(f)

        return out

    @classmethod
    def cdn_published_value(cls):
        # Return a value which should be used for cdn_published field.
        #
        # This method exists mainly to ensure this is mockable during tests.
        return datetime.datetime.utcnow()

    @step("Set cdn_published")
    def set_cdn_published(self, units):
        now = self.cdn_published_value()
        out = []
        for unit in units or []:
            out.append(
                self.pulp_client.update_content(attr.evolve(unit, cdn_published=now))
            )

        if out:
            LOG.info(
                "Setting cdn_published = %s on %s unit(s)",
                now,
                len(out),
            )
        return out

    def publish_with_cache_flush(self, repos, units=None):
        # Ensure all repos in 'repos' are fully published, and CDN/UD caches are flushed.
        #
        # If 'units' are provided, ensures those units have cdn_published field set after
        # the publish and before the UD cache flush.
        #
        units = units or []

        # publish the repos found
        publish_fs = self.publish(repos)

        # wait for the publish to complete before
        # flushing caches.
        f_sequence(publish_fs).result()

        # hook implementation(s) may now flush pulp-derived caches and datastores
        pm.hook.task_pulp_flush()

        # flush CDN cache
        out = self.flush_cdn(repos)

        # set units as published
        set_published = f_sequence(self.set_cdn_published(units))

        # flush UD cache only after cdn_published is set (if applicable)
        flush_ud = f_flat_map(set_published, lambda _: f_sequence(self.flush_ud(repos)))
        out.append(flush_ud)

        return out


class PushItemRecorder(CollectorService):
    @step("Record push items")
    def record_clears(self, cleared_repo_fs):
        return [f_flat_map(f, self.record_cleared_repo) for f in cleared_repo_fs]

    def record_cleared_repo(self, cleared_repo):
        push_items = []
        for task in cleared_repo.tasks:
            push_items.extend(self.push_items_for_task(task))
        return self.collector.update_push_items(push_items)

    def push_items_for_task(self, task):
        out = []
        for unit in task.units:
            push_item = self.push_item_for_unit(unit)
            if push_item:
                out.append(push_item)
        return out

    def push_item_for_unit(self, unit):
        for (unit_type, fn) in [
            (ModulemdUnit, self.push_item_for_modulemd),
            (RpmUnit, self.push_item_for_rpm),
            (FileUnit, self.push_item_for_file),
        ]:
            if isinstance(unit, unit_type):
                return fn(unit)

    def push_item_for_modulemd(self, unit):
        out = {}
        out["state"] = "DELETED"
        out["origin"] = "pulp"

        # Note: N:S:V:C:A format here is kept even if some part
        # of the data is missing (never expected to happen).
        # For example, if C was missing, you'll get N:S:V::A
        # so the arch part can't be misinterpreted as context.
        nsvca = ":".join(
            [unit.name, unit.stream, str(unit.version), unit.context, unit.arch]
        )

        out["name"] = nsvca

        return ModuleMdPushItem(**out)

    def push_item_for_rpm(self, unit):
        out = {}

        out["state"] = "DELETED"
        out["origin"] = "pulp"

        filename_parts = [
            unit.name,
            "-",
            unit.version,
            "-",
            unit.release,
            ".",
            unit.arch,
            ".rpm",
        ]
        out["name"] = "".join(filename_parts)

        # Note: in practice we don't necessarily expect to get all of these
        # attributes, as after a delete the server will only provide those
        # which make up the unit key. We still copy them anyway (even if
        # values are None) in case this is improved some day.
        out["sha256sum"] = unit.sha256sum
        out["md5sum"] = unit.md5sum
        out["signing_key"] = unit.signing_key

        return RpmPushItem(**out)

    def push_item_for_file(self, unit):
        out = {}

        out["state"] = "DELETED"
        out["origin"] = "pulp"
        out["name"] = unit.path
        out["sha256sum"] = unit.sha256sum

        return FilePushItem(**out)
