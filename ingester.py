"""
    ingester.py - top level ingestion algorithm.
"""


class Ingester(object):
    """
    Partially abstract base class for ingester objects. Needs to
    be subclassed and have dataset type and format specific functions
    defined before use.
    """

    def __init__(self, logger):
        """Set up a logger object."""

        self.logger = logger

    #
    # Top level algorithm
    #

    # pylint: disable=no-self-use
    #
    # These methods describe the top-level algorithm, which is the purpose
    # of this class. This is an algorithm-centric class, so it does not matter
    # if the methods do not use any class data.
    #

    def ingest(self, collection, source_dir):
        """Initiate the ingestion process.

        Find datasets under 'source_dir' and ingest them into 'collection'."""

        dataset_list = self.find_datasets(source_dir)

        for dataset_path in dataset_list:
            self.ingest_individual_dataset(collection, dataset_path)

        self.log_ingestion_process_complete(source_dir)

    def ingest_individual_dataset(self, collection, dataset_path):
        """Ingests a single dataset at 'dataset_path' into 'collection'."""

        try:
            metadata = self.parse_dataset_metadata(dataset_path)

            collection.check_metadata(metadata)

            self.ingest_transaction(collection, metadata)

        except DatasetError:
            self.log_dataset_skip(dataset_path)

        else:
            self.log_dataset_ingest_complete(dataset_path)

    def ingest_transaction(self, collection, metadata):
        """Ingests a single dataset identified by 'metadata'."""

        collection.begin_transaction()
        try:
            acquisition = collection.create_acquistion(metadata)

            dataset = acquisition.create_dataset(metadata)

            self.tile_dataset(dataset, metadata)

            dataset.mark_as_tiled()

        except:
            collection.rollback_transaction()
            raise

        else:
            collection.commit_transaction()

    def tile_dataset(self, dataset, metadata):
        """Tiles a dataset. The database entry is 'dataset', the source
        dataset is identified by 'metadata'."""

        for tile_type in dataset.list_tile_types():

            band_stack = self.stack_bands(tile_type, metadata)

            for tile_spec in tile_type.get_tile_coverage():

                self.make_tile(tile_spec, band_stack)

    def make_tile(self, tile_spec, band_stack):
        """Make a single tile. 'tile_spec' gives the tile footprint,
        'band_stack' is the untiled data."""

        tile_contents = tile_spec.create_tile_contents(band_stack)

        if tile_contents.has_data():
            tile = tile_spec.create_tile(tile_contents)
            self.make_mosaics(tile)

        else:
            tile_contents.remove()

    def make_mosaics(self, tile):
        """Create mosaics for a tile if it overlaps dataset boundaries."""

        for overlap in tile.list_overlaps():
            overlap.create_mosaic()

    #
    # Abstract methods
    #

    # pylint: disable=unused-argument
    #
    # These are abstract methods, designed to be overidden. They are
    # here to document what needs to be implemented in a subclass. The
    # do not do anything (except raise an exception), so they do not use
    # their arguments.

    def find_datasets(self, source_dir):
        """Return a list of path to the datasets under 'source_dir'.

        This is an abstract method since the method of identifying a
        dataset may vary between dataset formats.
        """

        assert False, "find_datasets abstract method not implemented."

    def parse_dataset_metadata(self, dataset_path):
        """Create and return a metadata object.

        dataset_path: points to the dataset to have its metadata read.

        Metadata objects differ for different types of dataset, but
        should present the same interface to the database objects. They
        include the dataset path.
        """

        assert False, "parse_dataset_metadata abstract method not implemented."

    def stack_bands(self, tile_type, metadata):
        """Create and return a band_stack object.

        tile_type - describes which bands to stack.
        metadata - has a path to the image data and additional information.
        """

        assert False, "stack_bands abstract method not implemented."

    # pylint: enable=unused-argument

    #
    # Log messages
    #

    # pylint: disable=missing-docstring
    #
    # These are simple and self documenting. They do not need docstrings.

    def log_ingestion_process_complete(self, source_dir):

        self.logger.info("Ingestion process complete for source directory " +
                         "'%s'." % source_dir)

    def log_dataset_skip(self, dataset_path):

        self.logger.info("Ingestion skipped for dataset " +
                         "'%s':" % dataset_path, exc_info=True)

    def log_dataset_ingest_complete(self, dataset_path):

        self.logger.info("Ingestion complete for dataset " +
                         "'%s'." % dataset_path)

    # pylint: enable=missing-docstring
    # pylint: enable=no-self-use


class DatasetError(Exception):
    """
    A problem specific to a dataset. If raised it will cause the
    current dataset to be skipped, but the ingest process will continue.
    """

    pass
