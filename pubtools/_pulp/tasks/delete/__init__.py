from .base import multi_dispatch
from .delete_packages import DeletePackages
from .delete_advisory import DeleteAdvisory


Delete = multi_dispatch("Delete", DeleteAdvisory, DeletePackages)


def entry_point(cls=Delete):
    with cls() as instance:
        instance.main()


def doc_parser():
    return DeletePackages().parser
