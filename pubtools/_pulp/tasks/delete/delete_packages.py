import logging

from functools import partial

from more_executors.futures import f_map
from pubtools.pulplib import Criteria, Matcher, RpmUnit, ModulemdUnit, FileUnit

from .base import DeleteTask, ClearedRepo

LOG = logging.getLogger("pubtools.pulp")


class DeletePackages(DeleteTask):
    def applicable(self, args):
        if args.filename:
            return True

    def run(self):
        # split content
        rpms = []
        modules = []
        files = []
        repos = {}
        rpm_fs = []
        mod_fs = []
        file_fs = []
        file_names = self.args.file
        for f in file_names:
            if f.endswith(".rpm"):
                rpms.append(f)
            elif f.endswith(".iso"):
                files.append(f)
            else:
                modules.append(f)

        # delete content
        if rpms:
            rpm_fs = self.delete_rpms(rpms)

        if files:
            file_fs = self.delete_files(files)

        if modules:
            mod_fs = self.delete_modules(modules)

        for f in rpm_fs + mod_fs + file_fs:
            repo = f.result().repo
            repos[repo.id] = repo

        # publish affected repos
        if repos:
            publish_fs = self.publish(repos.values())

            for f in publish_fs:
                f.result()

    def delete_rpms(self, rpm_names):
        # get content
        rpms = self.get_rpms(rpm_names)
        # map to repo
        repo_map = self.map_to_repo(rpms)

        # unassociate
        cleared_repo_fs = self.remove_rpms(repo_map)

        # log deletion
        if cleared_repo_fs:
            self.record_clears(cleared_repo_fs)

        # return repos updated
        return cleared_repo_fs

    def delete_files(self, file_names):
        # get isos
        files = self.get_files(file_names)
        # map to repo
        repo_map = self.map_to_repo(files)
        # delete isos
        cleared_repo_fs = self.delete_content(FileUnit, repo_map)
        # log deletion
        if cleared_repo_fs:
            self.record_clears(cleared_repo_fs)

        # return repos updated
        return cleared_repo_fs

    def delete_modules(self, module_names):
        cleared_repo_fs = []
        # search module
        modules = self.get_modules(module_names)

        if not modules:
            return []

        # delete the packages
        for module in modules:
            rpms = module.artifacts_filenames
            if rpms:
                cleared_repo_fs.extend(self.delete_rpms(rpms))

        # Wait for all the rpms to be deleted before deleting
        # the module
        if cleared_repo_fs:
            self.record_clears(cleared_repo_fs)
            [crf.result() for crf in cleared_repo_fs]

        # delete the module
        cleared_repo_fs = self.remove_modules(modules)

        # log deletion
        if cleared_repo_fs:
            self.record_clears(cleared_repo_fs)

        # return repos updated
        return cleared_repo_fs

    def get_files(self, file_names):
        files = self.search_files(file_names).result()
        self.check_files(file_names, files)

        return files

    def check_files(self, file_names, files):
        found = []
        for f in files:
            found.append(f.path)

        missing = set(file_names) - set(found)
        if missing:
            LOG.warn(
                "Requested iso(s) doesn't exist: %s", ", ".join(sorted(list(missing)))
            )

        total = len(found)
        for i, filename in enumerate(sorted(found)):
            LOG.info("Deleting [%4s/%4s] %s", i + 1, total, filename)

    def search_files(self, file_names):
        crit = []
        for file_name in file_names:
            crit.append(Criteria.with_field("name", file_name))

        criteria = Criteria.and_(Criteria.with_unit_type(FileUnit), Criteria.or_(*crit))

        return self.pulp_client.search_content(criteria=criteria)

    def remove_modules(self, modules):
        out = []
        repo_ids = self.args.repo
        repos = self.search_repo(repo_ids)

        for repo in repos:
            f = self.remove_modules_from_repo(repo, modules)
            f = f_map(f, partial(ClearedRepo, repo=repo, content=modules))
            f = f_map(f, self.log_remove)
            out.append(f)

        return out

    def remove_modules_from_repo(self, repo, modules):
        crit = []

        for module in modules:
            crit.append(Criteria.and_(*self._crit_from_mod_name(module.nsvca)))

        criteria = Criteria.and_(
            Criteria.with_unit_type(ModulemdUnit), Criteria.or_(*crit)
        )

        return repo.remove_content(criteria=criteria)

    def get_modules(self, module_names):
        modules = self.search_module(module_names).result()
        modules = self.check_modules(module_names, modules)
        return modules

    def check_modules(self, module_names, modules):
        found = {}
        skip = []
        repos = self.args.repo
        for module in modules:
            if not set(repos).issubset(module.repository_memberships):
                skip.append(module.nsvca)
            else:
                found[module.nsvca] = module

        missing = set(module_names) - set(found.keys() + skip)
        if missing:
            LOG.warn(
                "Requested module(s) doesn't exist: %s",
                ", ".join(sorted(list(missing))),
            )

        if skip:
            LOG.warn(
                "Requested module(s) not associated with one or more target repos: %s",
                ", ".join(sorted(skip)),
            )

        total = len(found)
        for i, mod in enumerate(sorted(found.items())):
            LOG.info("Deleting [%4s/%4s] %s", i + 1, total, mod[0])

        return found.values()

    def search_module(self, module_names):
        crit = []
        for module_name in module_names:
            crit.append(Criteria.and_(*self._crit_from_mod_name(module_name)))

        criteria = Criteria.and_(
            Criteria.with_unit_type(ModulemdUnit), Criteria.or_(*crit)
        )

        return self.pulp_client.search_content(criteria=criteria)

    def _crit_from_mod_name(self, module_name):
        crit = []
        mod_dict = self._get_nsvca(module_name)
        for m_part, value in mod_dict.items():
            crit.append(Criteria.with_field(m_part, value))
        return crit

    def _get_nsvca(self, module_name):
        mod_parts = ["name", "stream", "version", "context", "arch"]
        nsvca = module_name.split(":", 4)
        mod_dict = {}
        for i, p in enumerate(nsvca):
            if mod_parts[i] == "version":
                mod_dict[mod_parts[i]] = int(p)
            else:
                mod_dict[mod_parts[i]] = p

        return mod_dict

    def get_rpms(self, rpm_names):
        rpms = self.search_content(rpm_names, RpmUnit)
        self.check_missing_units(rpm_names, rpms)
        return rpms

    def remove_rpms(self, repo_map):
        return self.delete_content(RpmUnit, repo_map)

    def check_missing_units(self, unit_names, searched_units):
        found = {}
        for unit in searched_units:
            found.setdefault(unit.filename, []).append(unit.signing_key)

        missing = set(unit_names) - set(found.keys())
        if missing:
            missing = sorted(list(missing))
            LOG.warning("Requested unit(s) doesn't exist: %s", ", ".join(missing))

        total_units = len(found)
        for i, unit in enumerate(sorted(found.items())):
            LOG.info(
                "Deleting [%4s/%4s] %s (%s)",
                i + 1,
                total_units,
                unit[0],
                ", ".join(unit[1]),
            )

    def search_content(self, filenames, unit_type):
        file_crit = []
        signing_keys = self.args.signing_key
        signing_keys = [s.lower() for s in signing_keys] or [None]

        file_crit = self._files_criteria(filenames, signing_keys)

        criteria = Criteria.and_(
            Criteria.with_unit_type(unit_type), Criteria.or_(*file_crit)
        )

        search = self.pulp_client.search_content(criteria=criteria)

        return search.result()

    def _files_criteria(self, filenames, signing_keys):
        file_crit = []
        for filename in filenames:
            file_crit.append(
                Criteria.and_(
                    Criteria.with_field("filename", filename),
                    Criteria.with_field("signing_key", Matcher.in_(signing_keys)),
                )
            )
        return file_crit

    def map_to_repo(self, units):
        repo_map = {}
        repos = self.args.repo

        for unit in units:
            for repo in repos:
                if repo not in unit.repository_memberships:
                    LOG.warning(
                        "%s is not present in %s",
                        unit.filename,
                        repo,
                    )
                else:
                    repo_map.setdefault(repo, []).append(unit)

        missing = set(repos) - set(repo_map.keys())
        if missing:
            missing = ", ".join(sorted(list(missing)))
            LOG.warn("No units to remove from %s", missing)

        return repo_map

    def delete_content(self, unit_type, repo_map):
        out = []
        # get repos
        repos = self.search_repo(repo_map.keys())

        # request removal
        for repo in repos:
            units = repo_map.get(repo.id)
            f = self.remove_content_from_repo(repo, unit_type, units)
            f = f_map(f, partial(ClearedRepo, repo=repo, content=units))
            f = f_map(f, self.log_remove)
            out.append(f)

        return out

    def search_repo(self, repo_ids):
        return self.pulp_client.search_repository(Criteria.with_id(repo_ids)).result()

    def remove_content_from_repo(self, repo, unit_type, units):
        if unit_type == RpmUnit:
            crit = self._rpm_file_crit(units)
        elif unit_type == FileUnit:
            crit = self._file_crit(units)

        criteria = Criteria.and_(
            Criteria.with_unit_type(unit_type), Criteria.or_(*crit)
        )

        return repo.remove_content(criteria=criteria)

    def _rpm_file_crit(self, rpms):
        file_crit = []
        for rpm in rpms:
            file_crit.append(
                Criteria.and_(
                    Criteria.with_field("filename", rpm.filename),
                    Criteria.with_field("signing_key", rpm.signing_key),
                )
            )
        return file_crit

    def _file_crit(self, files):
        file_crit = []
        for file in files:
            file_crit.append(Criteria.with_field("name", file.path))
        return file_crit
