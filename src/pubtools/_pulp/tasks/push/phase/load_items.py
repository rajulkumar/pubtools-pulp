import logging
import attr

from pushsource import Source

from .base import Phase
from ..items import PulpPushItem


LOG = logging.getLogger("pubtools.pulp")


class LoadPushItems(Phase):
    """Phase for loading push items.

    This should be the very first phase, as it is responsible for discovering
    the content which should be operated on by all the later phases.

    Input queue:
    - none.

    Output queue:
    - items which should be pushed by this task. The items might be missing
      checksums and will definitely be missing any info regarding the Pulp state.

    Side-effects:
    - populates item_info on the context.
    """

    def __init__(self, context, source_urls, allow_unsigned, pre_push, **_):
        super(LoadPushItems, self).__init__(
            context,
            name="Load push items",
            # This phase needs to customize output queue generation to remove any max size.
            #
            # Reasoning: as the very first phase, we are responsible for calculating some
            # important info about the push (e.g. total number of items). We want to calculate
            # that as soon as we possibly can. So we don't want any backpressure from later
            # phases here.
            out_queue=context.new_queue(maxsize=0),
        )
        self._source_urls = source_urls
        self._allow_unsigned = allow_unsigned
        self._pre_push = pre_push

    def check_signed(self, item):
        if item.supports_signing and not item.is_signed and not self._allow_unsigned:
            raise RuntimeError(
                "Unsigned content is not permitted: %s" % item.pushsource_item.src
            )

    @property
    def raw_items(self):
        for source_url in self._source_urls:
            with Source.get(source_url) as source:
                LOG.info("Loading items from %s", source_url)
                for item in source:
                    yield item

    @property
    def filtered_items(self):
        for item in self.raw_items:
            # The destination can possibly contain a mix of Pulp repo IDs
            # and absolute paths. Paths occur at least in the Errata Tool
            # case, as used for FTP push.
            #
            # In this command we only want to deal with repo IDs, so we'll
            # filter out the rest.
            dest = [val for val in item.dest if "/" not in val]

            pulp_item = PulpPushItem.for_item(attr.evolve(item, dest=dest))

            if not pulp_item:
                LOG.info("Skipping unsupported type: %s", item)
                continue

            # If there is no destination at all (nowhere to push it...)
            if not dest:
                if self._pre_push and pulp_item and pulp_item.can_pre_push:
                    # Lack of dest is OK in the pre-push case, if the item
                    # supports that. For example, for an RPM it means it'll just
                    # go into all-rpm-content, which doesn't need any 'dest'.
                    pass
                else:
                    # In other cases, no dest means no way to push it.
                    # The item is therefore skipped, and this is unusual enough
                    # to warn about it.
                    LOG.warning("Skipping item with no destination: %s", item)
                    continue

            yield pulp_item

    def run(self):
        for pulp_item in self.filtered_items:
            # Since there is no input queue, increment our input count explicitly.
            self.progress_info.incr_in()

            # Also record the item on the context.
            self.context.item_info.add_item(pulp_item)

            self.check_signed(pulp_item)
            self.put_output(pulp_item)

            LOG.debug("Loaded item: %s", pulp_item)

        # We know by now that there are no more items to add onto the context.
        self.context.item_info.items_known.set()
