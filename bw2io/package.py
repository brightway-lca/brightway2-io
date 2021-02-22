# -*- coding: utf-8 -*-
from .errors import UnsafeData, InvalidPackage
from .validation import bw2package_validator
from bw2data import projects
from bw2data.logs import get_logger
from bw2data.serialization import JsonWrapper, JsonSanitizer
from bw2data.utils import download_file
from bw2data.utils import safe_filename
from time import time
from voluptuous import Invalid
import os
import warnings


class BW2Package(object):
    """This is a format for saving objects which implement the :ref:`datastore` API. Data is stored as a BZip2-compressed file of JSON data. This archive format is compatible across Python versions, and is, at least in theory, programming-language agnostic.

    Validation is done with ``bw2data.validate.bw2package_validator``.

    The data format is:

    .. code-block:: python

        {
            'metadata': {},  # Dictionary of metadata to be written to metadata-store.
            'name': basestring,  # Name of object
            'class': {  # Data on the underlying class. A new class is instantiated
                        # based on these strings. See _create_class.
                'module': basestring,  # e.g. "bw2data.database"
                'name': basestring  # e.g. "Database"
            },
            'unrolled_dict': bool,  # Flag indicating if dictionary keys needed to
                                    # be modified for JSON (as JSON keys can't be tuples)
            'data': object  # Object data, e.g. LCIA method or LCI database
        }

    Perfect roundtrips between machines are not guaranteed:
        * All lists are converted to tuples (because JSON does not distinguish between lists and tuples).
        * Absolute filepaths in metadata would be specific to a certain computer and user.

    .. note:: This class does not need to be instantiated, as all its methods are ``classmethods``, i.e. do ``BW2Package.import_obj("foo")`` instead of ``BW2Package().import_obj("foo")``

    """

    APPROVED = {
        "bw2calc",
        "bw2data",
        "bw2io",
        "bw2regional",
        "bw2temporalis",
    }

    @classmethod
    def _get_class_metadata(cls, obj):
        return {"module": obj.__class__.__module__, "name": obj.__class__.__name__}

    @classmethod
    def _is_valid_package(cls, data):
        try:
            bw2package_validator(data)
            return True
        except Invalid:
            return False

    @classmethod
    def _is_whitelisted(cls, metadata):
        return metadata["module"].split(".")[0] in cls.APPROVED

    @classmethod
    def _create_class(cls, metadata, apply_whitelist=True):
        if apply_whitelist and not cls._is_whitelisted(metadata):
            raise UnsafeData(
                "{}.{} not a whitelisted class name".format(
                    metadata["module"], metadata["name"]
                )
            )
        # Compatibility with bw2data version 1
        if metadata["module"] == "bw2data.backends.default.database":
            metadata["module"] = "bw2data.backends.single_file.database"
        exec("from {} import {}".format(metadata["module"], metadata["name"]))
        return locals()[metadata["name"]]

    @classmethod
    def _prepare_obj(cls, obj, backwards_compatible=False):
        ds = {
            "metadata": obj.metadata,
            "name": obj.name,
            "class": cls._get_class_metadata(obj),
            "data": obj.load(),
        }
        if backwards_compatible:
            if ds["class"]["module"] in (
                "bw2data.backends.single_file.database",
                "bw2data.backends.peewee.database",
            ):
                ds["class"]["module"] = "bw2data.backends.default.database"
                ds["class"]["name"] = "SingleFileDatabase"
            ds["metadata"].pop("backend", None)
            ds["metadata"].pop("searchable", None)
        return ds

    @classmethod
    def _load_obj(cls, data, whitelist=True):
        if not cls._is_valid_package(data):
            raise InvalidPackage
        data["class"] = cls._create_class(data["class"], whitelist)
        return data

    @classmethod
    def _create_obj(cls, data):
        instance = data["class"](data["name"])

        if data["name"] not in instance._metadata:
            instance.register(**data["metadata"])
        else:
            instance.backup()
            instance.metadata = data["metadata"]

        instance.write(data["data"])
        return instance

    @classmethod
    def _write_file(cls, filepath, data):
        JsonWrapper.dump_bz2(JsonSanitizer.sanitize(data), filepath)

    @classmethod
    def export_objs(cls, objs, filename, folder="export", backwards_compatible=False):
        """Export a list of objects. Can have heterogeneous types.

        Args:
            * *objs* (list): List of objects to export.
            * *filename* (str): Name of file to create.
            * *folder* (str, optional): Folder to create file in. Default is ``export``.
            * *backwards_compatible* (bool, optional): Create package compatible with bw2data version 1.

        Returns:
            Filepath of created file.

        """
        filepath = os.path.join(
            projects.request_directory(folder), safe_filename(filename) + u".bw2package"
        )
        cls._write_file(
            filepath, [cls._prepare_obj(o, backwards_compatible) for o in objs]
        )
        return filepath

    @classmethod
    def export_obj(
        cls, obj, filename=None, folder="export", backwards_compatible=False
    ):
        """Export an object.

        Args:
            * *obj* (object): Object to export.
            * *filename* (str, optional): Name of file to create. Default is ``obj.name``.
            * *folder* (str, optional): Folder to create file in. Default is ``export``.
            * *backwards_compatible* (bool, optional): Create package compatible with bw2data version 1.

        Returns:
            Filepath of created file.

        """
        if filename is None:
            filename = obj.filename
        return cls.export_objs([obj], filename, folder, backwards_compatible)

    @classmethod
    def load_file(cls, filepath, whitelist=True):
        """Load a bw2package file with one or more objects. Does not create new objects.

        Args:
            * *filepath* (str): Path of file to import
            * *whitelist* (bool): Apply whitelist of approved classes to allowed types. Default is ``True``.

        Returns the loaded data in the bw2package dict data format, with the following changes:
            * ``"class"`` is an actual Python class object (but not instantiated).

        """
        raw_data = JsonSanitizer.load(JsonWrapper.load_bz2(filepath))
        if isinstance(raw_data, dict):
            return cls._load_obj(raw_data, whitelist)
        else:
            return [cls._load_obj(o, whitelist) for o in raw_data]

    @classmethod
    def import_file(cls, filepath, whitelist=True):
        """Import bw2package file, and create the loaded objects, including registering, writing, and processing the created objects.

        Args:
            * *filepath* (str): Path of file to import
            * *whitelist* (bool): Apply whitelist to allowed types. Default is ``True``.

        Returns:
            Created object or list of created objects.

        """
        loaded = cls.load_file(filepath, whitelist)
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            if isinstance(loaded, dict):
                return cls._create_obj(loaded)
            else:
                return [cls._create_obj(o) for o in loaded]


def download_biosphere():
    logger = get_logger("io-performance.log")
    start = time()
    filepath = download_file("biosphere-new.bw2package")
    logger.info("Downloading biosphere package: %.4g" % (time() - start))
    start = time()
    BW2Package.import_file(filepath)
    logger.info("Importing biosphere package: %.4g" % (time() - start))


def download_methods():
    logger = get_logger("io-performance.log")
    start = time()
    filepath = download_file("methods-new.bw2package")
    logger.info("Downloading methods package: %.4g" % (time() - start))
    start = time()
    BW2Package.import_file(filepath)
    logger.info("Importing methods package: %.4g" % (time() - start))
