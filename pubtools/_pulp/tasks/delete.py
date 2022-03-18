import logging
import attr
import re

from functools import partial

from more_executors.futures import f_map
from pubtools.pulplib import Criteria, Matcher, RpmUnit, ModulemdUnit, FileUnit

from pubtools._pulp.services.pulp import PulpClientService

from pubtools._pulp.task import PulpTask
from pubtools._pulp.tasks.common import Publisher
from pubtools._pulp.arguments import SplitAndExtend

LOG = logging.getLogger("pubtools.pulp")

step = PulpTask.step

MODULEMD_REGEX = re.compile(r'^[-.+\w]+:[-.+\w]+:\d+(:[-.+\w]+){0,2}$')

@attr.s
class ClearedRepo(object):
    """Represents a single repo where contents were removed."""

    tasks = attr.ib()
    """The completed Pulp tasks for removing content from this repo."""

    repo = attr.ib()
    """The repo which where content was cleared."""

    content = attr.ib()
    """The content that was cleared from the repo"""


class Delete(PulpTask, PulpClientService, Publisher):
    def add_args(self):
        super(Delete, self).add_args()

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

    def run(self):
        # separates files into rpms, isos and modules
        rpms, files, modules, skipped = self.separate_files(self.args.file)

        # collect skipped items

        # delete rpms

        # delete files

        # delete modules

        # delete packages from advisory

        # publish affected repos


    def delete_rpms(self, rpm_names, signing_keys, repos):
        missing = []
        # get rpms from Pulp
        rpms_f = self.get_rpms(rpm_names, signing_keys)
        # map rpms to repos
        f = f_map(rpms_f, partial(self.map_to_repos(repos=repos)))
        repo_map_f = f_map(f, self.log_units(f))
        # remove rpms from repos
        cleared_repos_f = self.delete_rpms(repo_map_f)
        # collect items

        # return affected repos


    def log_units(repo_map, unit_map):
        missing = []
        for unit, repos in unit_map.items():
            if not repos:
                missing.appned(unit)
            LOG.info("Deleting %s from %s", unit, repos)

        if missing:
            LOG.warn("Unit(s) doesn't belong to any requested repos %s: %s", repos, ",".join(sorted(missing)))
            #TODO: collect units "SKIPPING"

        return repo_map    

    def get_rpms(self, rpm_names, signing_keys):
        criteria = self.unit_criteria(RpmUnit, self.rpm_search_criteria(rpm_names, signing_keys))
        rpms_f = self.search_content(criteria)
        rpms_f = f_map(rpms_f, partial(self.check_missing_units(unit_names=rpm_names)))
        return rpms_f

    def unit_criteria(self, unit_type, partial_crit):
        criteria = Criteria.and_(
            Criteria.with_unit_type(unit_type), Criteria.or_(*partial_crit)
        )
        return criteria
    
    def _rpm_search_criteria(self, rpm_names, signing_keys):
        part_crit = []
        signing_keys = [s.lower() for s in signing_keys] or [None]
        
        for rpm_name in rpm_names:
            part_crit.append(self._rpm_criteria(rpm_name, signing_keys))
        
        return part_crit

    def _rpm_criteria(self, filename, signing_keys):
        return Criteria.and_(
                    Criteria.with_field("filename", filename),
                    Criteria.with_field("signing_key", Matcher.in_(signing_keys)),
                )

    def search_content(self, criteria):
        return self.pulp_client.search_content(criteria=criteria)

    def check_missing_units(self, unit_names, searched_units):
        found = []
        for unit in searched_units:
            found.setdefault(unit.filename, []).append(unit.signing_key)

        missing = set(unit_names) - set(found.keys())
        if missing:
            missing = sorted(list(missing))
            LOG.warning("Requested unit(s) doesn't exist: %s", ", ".join(missing))

        
        # TODO: collect missing units "NOT FOUND"
        """
        total_units = len(found)
        for i, unit in enumerate(sorted(found.items())):
            LOG.info(
                "Deleting [%4s/%4s] %s (%s)",
                i + 1,
                total_units,
                unit[0],
                ", ".join(unit[1]),
            )
        """ 
        return found 


    def map_to_repo(self, units, repos):
        repo_map = {}
        unit_map = {}

        for unit in units:
            unit_map.setdefault(unit.name, [])
            for repo in repos:
                if repo not in unit.repository_memberships:
                    LOG.warning(
                        "%s is not present in %s",
                        unit.filename,
                        repo,
                    )
                else:
                    repo_map.setdefault(repo, []).append(unit)
                    unit_map.get(unit.name, []).appned(repo)

        missing = set(repos) - set(repo_map.keys())
        if missing:
            missing = ", ".join(sorted(list(missing)))
            LOG.warn("No units to remove from %s", missing)

        return repo_map

    def delete_rpms(self, repo_map):
        return self.delete_content(RpmUnit, repo_map, self._rpm_remove_crit)


    def delete_content(self, unit_type, repo_map, criteria_fn):
        out = []
        # get repos
        repos = self.search_repo(repo_map.keys())

        # request removal
        for repo in repos:
            units = repo_map.get(repo.id)
            criteria = self.unit_criteria(unit_type, criteria_fn(units))
            f = self.repo.remove_content(criteria=criteria)
            f = f_map(f, partial(ClearedRepo, repo=repo, content=units))
            f = f_map(f, self.log_remove)
            out.append(f)

        return out


    def _rpm_remove_crit(self, units):
        part_crit = []
        for unit in units:
            part_crit.append(self._rpm_criteria(unit.name, unit.signing_key))

        return part_crit

    def search_repo(self, repo_ids):
        return self.pulp_client.search_repository(Criteria.with_id(repo_ids)).result()


    def separate_files(self, files):
        rpms = []
        modules = []
        files = []
        skipped = []
        for f in files:
            if f.endswith(".rpm"):
                rpms.append(f)
            elif f.endswith(".iso"):
                files.append(f)
            elif self.is_valid_modulemd(f):
                modules.append(f)
            else:
                skipped.append(f)
                LOG.warning("Skipping %s: not a valid file for removal", f)
        return rpms, files, modules, skipped
        

    def is_valid_modulemd(file):
        if MODULEMD_REGEX.match(file):
            return True  
        return False

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

    
    