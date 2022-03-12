import logging
import attr

from functools import partial

from more_executors.futures import f_map
from pushsource import Source, PushItem
from pubtools.pulplib import Criteria, ErratumUnit, RpmUnit

from .base import DeleteTask, ClearedRepo

LOG = logging.getLogger("pubtools.pulp")


class DeleteAdvisory(DeleteTask):
    def applicable(self, args):
        if args.advisory and not args.file:
            return True

    def run(self):
        pushitems = []
        advisory_id = None

        repo_names = self.args.repo

        # get errata from source
        with Source.get(self.args.advisory) as source:
            for item in source:
                if isinstance(item, PushItem):
                    pushitems.append(item)
                elif not advisory_id:
                    advisory_id = item.split(": ")[0]

        # get advisory from pulp
        advisory = self.get_advisory(advisory_id)

        # process advisory pushitems from errata
        repo_map = self.process_pushitems(pushitems, repo_names)

        # update map with repo in errata
        repo_map = self.verify_repos_from_advisory(advisory, repo_map)

        # delete items
        cleared_repo_fs = self.delete_items(repo_map)

        # record pushitems and publish repos
        if cleared_repo_fs:
            self.record_clears(cleared_repo_fs)

            for crf in cleared_repo_fs:
                repo = crf.result().repo
                self.publish([repo])

    def get_repos(self, repo_ids):
        repos = []
        criteria = Criteria.with_id(repo_ids)
        search = self.pulp_client.search_repository(criteria=criteria)
        for repo in search.result():
            repos.append(repo)
        return repos

    def delete_items(self, repo_map):
        out = []
        # get repos from pulp
        repos = self.get_repos(repo_map.keys())

        for repo in repos:
            items = repo_map.get(repo.id)
            f = self.remove_content(repo, items)
            f = f_map(f, partial(ClearedRepo, repo=repo, content=items))
            f = f_map(f, self.log_remove)
            out.append(f)

        return out

    def remove_content(self, repo, items):
        crit = []
        for item in items:
            crit.append(
                Criteria.and_(
                    Criteria.with_field("filename", item.name),
                    Criteria.with_field("signing_key", item.signing_key.lower()),
                )
            )

        criteria = Criteria.and_(Criteria.with_unit_type(RpmUnit), Criteria.or_(*crit))

        return repo.remove_content(criteria=criteria)

    def verify_repos_from_advisory(self, advisory, repo_map):
        repo_membership = advisory.repository_memberships

        for repo_name in repo_map.keys():
            if repo_name not in repo_membership:
                LOG.warn("Advisory %s is not in repo %s", advisory.id, repo_name)
                repo_map.remove(repo_name)

        return repo_map

    def process_pushitems(self, pushitems, repos):

        repo_map = {}

        # repos = self.args.repos

        for item in pushitems:
            mapped = False
            for repo in repos or item.dest:
                if repo in item.dest:
                    repo_map.setdefault(repo, []).append(item)
                    mapped = True

            if not mapped:
                LOG.warn(
                    "%s is not present in any repo %s", item.name, ", ".join(repos)
                )
                item = attr.evolve(item, state="SKIPPED")

        return repo_map

    def get_advisory(self, advisory_id):
        advisory = self.search_advisory(advisory_id).result()
        advisory = [a for a in advisory]
        if not advisory:
            LOG.error("Advisory %s not found", advisory_id)
            return

        return advisory[0]

    def search_advisory(self, advisory_id):
        criteria = Criteria.and_(
            Criteria.with_unit_type(ErratumUnit), Criteria.with_field("id", advisory_id)
        )

        return self.pulp_client.search_content(criteria=criteria)
