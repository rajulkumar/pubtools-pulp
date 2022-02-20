import logging
import sys


from .contextlib_compat import exitstack
from .phase import (
    LoadPushItems,
    LoadChecksums,
    QueryPulp,
    Upload,
    EndPrePush,
    Update,
    Collect,
    Associate,
    Publish,
    Context,
)
from ..common import Publisher, PulpTask
from ...services import (
    CollectorService,
    PulpClientService,
)

step = PulpTask.step

LOG = logging.getLogger("pubtools.pulp")


# Because pylint misunderstands the type of e.g. pulp_client:
# E1101: Instance of 'CollectorProxy' has no 'search_repository' member (no-member)
#
# pylint: disable=no-member


class Push(
    CollectorService,
    PulpClientService,
    Publisher,
    PulpTask,
):
    """Push and publish content via Pulp."""

    def add_args(self):
        super(Push, self).add_args()

        self.add_publisher_args(self.parser)

        self.parser.add_argument(
            "--pre-push",
            action="store_true",
            help=(
                "Pre-push mode: do as much as possible without making content "
                "available to end-users, then stop. May be used to improve the "
                "performance of a subsequent full push."
            ),
        )

        self.parser.add_argument(
            "--source", action="append", help="Source(s) of content to be pushed"
        )

    def run(self):
        # Push workflow.
        #
        # Push is separated into various phases. Each phase has one thread
        # associated with it, and generally is connected to the next phase by
        # a queue.
        phases = []

        # This is a context object shared by all phases.
        ctx = Context()

        # Prepare pushcollector 'phase'. This phase is a bit special in that
        # it runs in parallel to all other phases, and its input queue is written
        # to by all other phases.
        collect_phase = Collect(context=ctx, collector=self.collector)
        phases.append(collect_phase)

        # A helper to add a phase with consistent initialization.
        def add_phase(klass, **kwargs):
            if "in_queue" not in kwargs and phases:
                # For all phases except the first, the input queue defaults
                # to the previous phase's output queue, i.e. each phase passes
                # some items onto the next via the queue.
                kwargs["in_queue"] = phases[-1].out_queue

            # Provide many default arguments to each phase.
            # Phases accept any number of keyword arguments, so these aren't
            # all used by every phase.
            kwargs.update(
                context=ctx,
                pulp_client=self.pulp_client,
                pre_push=self.args.pre_push,
                update_push_items=collect_phase.update_push_items,
                publish_with_cache_flush=self.publish_with_cache_flush,
            )
            phases.append(klass(**kwargs))

        # Now proceed with adding the phases which make up a push...

        # Load push items from pushsource library.
        # As the first phase, this does not have an input queue as it obtains
        # its inputs from pushsource library.
        add_phase(LoadPushItems, in_queue=None, source_urls=self.args.source)

        # Ensure we have checksums for each push item. Potentially involves
        # reading content for push over NFS.
        add_phase(LoadChecksums)

        # Figure out the current state of each item in Pulp.
        add_phase(QueryPulp)

        # Ensure all items are uploaded to Pulp. This uploads bytes into Pulp
        # but does not guarantee the items are present in each of the desired
        # destination repos.
        add_phase(Upload)

        if self.args.pre_push:
            # If we are in pre-push mode then we do not go any further, we just wait
            # for all previous steps, then log a message and exit.
            add_phase(EndPrePush)

        else:
            # Ensure all items are up-to-date in Pulp. This adjusts any mutable fields
            # whose current value doesn't match the desired value.
            add_phase(Update)

            # Ensure all items are associated into the desired target repos.
            add_phase(Associate)

            # Ensure all repos are published once the desired content is present.
            add_phase(Publish)

        # We've connected up all phases of the push, now we just need to
        # start them all.
        #
        # This will start all the phases...
        with exitstack([ctx.progress_logger()] + phases):
            LOG.debug("All push phases are now running.")
            # ...and exiting the 'with' block here will wait for them to
            # complete.

        # If a phase failed, it's communicated back to us through the
        # context object here. Exit unsuccessfully if so.
        if ctx.has_error:
            LOG.error("Push failed with fatal error, see previous logs.")
            sys.exit(59)
