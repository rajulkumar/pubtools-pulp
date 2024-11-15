import os
import logging

import yaml
import attr
from frozendict import frozendict
from frozenlist2 import frozenlist
from pubtools.pluggy import pm, hookimpl

from pubtools import pulplib
from pubtools.pulplib import FakeController, FileRepository, YumRepository

LOG = logging.getLogger("pubtools-pulp")


def default_value_match(obj, field, field_value):
    if not field:
        # No field, hence no default value
        return False

    if field.default is attr.NOTHING:
        # Field does not use a default, hence no match
        return False

    if isinstance(field.default, attr.Factory):
        if field.default.takes_self:
            default = field.default.factory(obj)
        else:
            default = field.default.factory()
        return field_value == default

    return field_value == field.default


def serialize(value):
    """Serialize pulplib model objects to a form which can be stored
    in YAML and later deserialized.
    """

    if isinstance(value, list):
        return [serialize(elem) for elem in value]

    if isinstance(value, (dict, frozendict)):
        out = {}
        for key, elem in value.items():
            out[key] = serialize(elem)
        return out

    if attr.has(type(value)):
        # We do not use the recursion feature in asdict because it
        # doesn't put enough metadata in the output for deserialization
        # to work (and attr library itself doesn't provide an inverse
        # of asdict either).
        fields = attr.fields(type(value))
        out = attr.asdict(value, recurse=False)
        out["_class"] = type(value).__name__

        # Private attrs field which cannot be (de)serialized
        if "_client" in out:
            del out["_client"]

        del_keys = []
        for key in out.keys():
            field = getattr(fields, key, None)
            if default_value_match(value, field, out[key]):
                # Do not serialize fields whose values are simply the default.
                # This helps keep the state file terse and also allows the data
                # to be "upgraded" as defaults change in pulplib.
                del_keys.append(key)
            else:
                out[key] = serialize(out[key])

        for key in del_keys:
            del out[key]

        return out

    return value


def deserialize(value):
    """Inverse of 'serialize'."""

    if isinstance(value, list):
        return [deserialize(elem) for elem in value]

    if isinstance(value, dict) and "_class" not in value:
        # Plain old dict
        out = {}
        for key, elem in value.items():
            out[key] = deserialize(elem)
        return out

    if isinstance(value, dict) and "_class" in value:
        value = value.copy()

        model_class = getattr(pulplib, value.pop("_class"))
        assert attr.has(model_class)

        # Deserialize everything inside it first using the plain dict
        # logic. This is where we recurse into nested attr classes, if any.
        value = deserialize(value)

        return model_class(**value)

    return value


def yaml_dumper(*args, **kwargs):
    # Returns a yaml.SafeDumper which also supports immutable containers of the
    # types used by pubtools-pulplib objects.
    out = yaml.SafeDumper(*args, **kwargs)
    out.add_representer(frozendict, out.__class__.represent_dict)
    out.add_representer(frozenlist, out.__class__.represent_list)
    return out


class PersistentFake(object):
    """Wraps pulplib fake client adding persistence of state."""

    def __init__(self, state_path):
        self.ctrl = FakeController()
        self.state_path = state_path

        # Register ourselves with pubtools so we can get the task stop hook,
        # at which point we will save our current state.
        pm.register(self)

    def load_initial(self):
        """Initial load of data into the fake, in the case where no state
        has previously been persisted.

        This will populate a hardcoded handful of repos which are expected
        to always be present in a realistically configured rhsm-pulp server.
        """
        self.ctrl.insert_repository(FileRepository(id="redhat-maintenance"))
        self.ctrl.insert_repository(FileRepository(id="all-iso-content"))
        self.ctrl.insert_repository(YumRepository(id="all-rpm-content"))
        # Repos required for common test data (RHELDST-23264)
        self.ctrl.insert_repository(YumRepository(id="all-rpm-content-e8"))
        self.ctrl.insert_repository(YumRepository(id="all-rpm-content-54"))
        self.ctrl.insert_repository(YumRepository(id="all-erratum-content-2019"))
        self.ctrl.insert_repository(YumRepository(id="all-erratum-content-2020"))

    def load(self):
        """Load data into the fake from previously serialized state (if any).

        If no state has been previously serialized, load_initial will be used
        to seed the fake with some hardcoded state.
        """

        if not os.path.exists(self.state_path):
            return self.load_initial()

        with open(self.state_path, "rt") as f:  # pylint:disable=unspecified-encoding
            raw = yaml.load(f, Loader=yaml.SafeLoader)

        repos = raw.get("repos") or []
        for repo in deserialize(repos):
            self.ctrl.insert_repository(repo)

        units = raw.get("units") or []
        for unit in deserialize(units):
            for repo_id in unit.repository_memberships:
                repo = self.ctrl.client.get_repository(repo_id).result()
                self.ctrl.insert_units(repo, [unit])

    def save(self):
        """Serialize the current state of the fake and save it to persistent storage."""

        serialized = {}

        serialized["repos"] = serialize(self.ctrl.repositories)
        serialized["repos"].sort(key=lambda repo: repo["id"])

        all_units = list(self.ctrl.client.search_content())
        serialized["units"] = serialize(all_units)

        # This sort key is a bit expensive since it means we essentially do yaml dump
        # twice. On the plus side it ensures a stable order across py2 and py3.
        serialized["units"].sort(key=lambda x: yaml.dump(x, Dumper=yaml_dumper))

        path = self.state_path

        state_dir = os.path.dirname(path)
        if not os.path.isdir(state_dir):
            os.makedirs(state_dir)

        with open(path, "wt") as f:  # pylint:disable=unspecified-encoding
            yaml.dump(serialized, f, Dumper=yaml_dumper)

        LOG.info("Fake pulp state persisted to %s", path)

    @hookimpl
    def task_stop(self, failed):  # pylint:disable=unused-argument
        """Called when a task is ending."""
        pm.unregister(self)
        self.save()


def new_fake_controller(state_path=None):
    """Create and return a new fake Pulp controller.

    On top of the fake built in to pulplib library, this adds persistent state
    stored under ~/.config/pubtools-pulp by default.

    The state is persisted in a somewhat human-accessible form; the idea is that
    you can manually view and edit the YAML to see how the commands behave.
    """
    state_path = state_path or os.path.expanduser("~/.config/pubtools-pulp/fake.yaml")
    fake = PersistentFake(state_path)
    fake.load()
    return fake.ctrl
