from ast import Del
import logging

import attr

from ...arguments import SplitAndExtend
from ..common import Publisher, PulpTask, PushItemRecorder
from ...services import (
    CollectorService,
    PulpClientService,
)

LOG = logging.getLogger("pubtools.pulp")

step = PulpTask.step


@attr.s
class ClearedRepo(object):
    """Represents a single repo where contents were removed."""

    tasks = attr.ib()
    """The completed Pulp tasks for removing content from this repo."""

    repo = attr.ib()
    """The repo which where content was cleared."""

    content = attr.ib()
    """The content that was cleared from the repo"""


class DeleteTask(PulpTask, PulpClientService, PushItemRecorder, Publisher):
    """Delete content via Pulp"""

    def add_args(self):
        super(DeleteTask, self).add_args()

        self.add_publisher_args(self.parser)

        self.parser.add_argument(
            "--repo",
            help="remove content from these comma-seperated repositories ",
            type=str,
            action=SplitAndExtend,
            split_on=",",
        )

        self.parser.add_argument(
            "--advisory",
            help="remove packages in the advisory",
            type=str,
        )

        self.parser.add_argument(
            "--file",
            help="remove these comma-separated rpm, srpm, modulemd or iso file(s)",
            type=str,
            action=SplitAndExtend,
            split_on=",",
        )

        self.parser.add_argument(
            "--signing-key",
            help="remove the content with these signing-keys(s)",
            type=str,
            action=SplitAndExtend,
            split_on=",",
        )

    def log_remove(self, removed_repo):
        # Given a repo which has been cleared, log some messages
        # summarizing the removed unit(s)
        content_types = {}

        for task in removed_repo.tasks:
            for unit in task.units:
                type_id = unit.content_type_id
                content_types[type_id] = content_types.get(type_id, 0) + 1

        task_ids = ", ".join(sorted([t.id for t in removed_repo.tasks]))
        repo_id = removed_repo.repo.id
        if not content_types:
            LOG.warning("%s: no content removed, tasks: %s", repo_id, task_ids)
        else:
            removed_types = []
            for key in sorted(content_types.keys()):
                removed_types.append("%s %s(s)" % (content_types[key], key))
            removed_types = ", ".join(removed_types)

            LOG.info("%s: removed %s, tasks: %s", repo_id, removed_types, task_ids)

        return removed_repo


def multi_dispatch_class(classes):
    class Out(DeleteTask):
        def __init__(self, *args, **kwargs):
            super(Out, self).__init__(*args, **kwargs)
            # self.args

            for klass in classes:
                applicable = getattr(klass, "applicable", lambda _: True)
                if applicable(self.args):
                    self.run = klass.run
                    return

    return Out


def multi_dispatch(name, *classes):
    if len(classes) == 0:
        raise AssertionError("BUG: cannot multi_dispatch zero classes!")

    if len(classes) == 1:
        return classes[0]

    for klass in classes:
        if not issubclass(klass, DeleteTask):
            raise AssertionError("BUG: multi_dispatch class must inherit DeleteTask")

    out = multi_dispatch_class(classes)
    out.__name__ = name

    return out
