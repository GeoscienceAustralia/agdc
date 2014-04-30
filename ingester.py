"""
    ingester.py - top level ingestion algorithm.
"""

from abc import ABCMeta, abstractmethod


class Ingester(object):
    """
    Partially abstract base class for ingester objects. Needs to
    be subclassed and have dataset type and format specific functions
    defined before use.
    """

    #
    # Declare this as an abstract base class, allowing the use of the
    # abstractmethod decorator.
    #
    __metaclass__ = ABCMeta

    def __init__(self, logger, collection):
        """Set up the ingester object.

        logger: the logger object to which log messages will be sent.
        collection: The datacube collection which will accept the ingest.
        """

        self.logger = logger
        self.collection = collection

    #
    # Top level algorithm
    #
    # These methods describe the top-level algorithm, which is the purpose
    # of this class.
    #

    def ingest(self, source_dir):
        """Initiate the ingestion process.

        Find datasets under 'source_dir' and ingest them into the collection.
        """

        dataset_list = self.find_datasets(source_dir)

        for dataset_path in dataset_list:
            self.ingest_individual_dataset(dataset_path)

        self.log_ingestion_process_complete(source_dir)

    def ingest_individual_dataset(self, dataset_path):
        """Ingests a single dataset at 'dataset_path' into the collection.

        If this process raises a DatasetError, the dataset is skipped,
        but the process continues.
        """

        try:
            dataset = self.open_dataset(dataset_path)

            self.collection.check_metadata(dataset)

            self.ingest_transaction(dataset)

        except DatasetError:
            self.log_dataset_skip(dataset_path)

        else:
            self.log_dataset_ingest_complete(dataset_path)

    def ingest_transaction(self, dataset):
        """Ingests a single dataset into the collection.

        This is done in a single transaction: if anything goes wrong
        the transaction is rolled back and no changes are made, then
        the error is propagated.
        """

        self.collection.begin_transaction()
        try:

            acquisition_record = \
                self.collection.create_acquisition_record(dataset)
            dataset_record = acquisition_record.create_dataset_record(dataset)

            self.tile_dataset(dataset_record, dataset)

            dataset_record.mark_as_tiled()

        except:
            self.collection.rollback_transaction()
            raise

        else:
            self.collection.commit_transaction()

    def tile_dataset(self, dataset_record, dataset):
        """Tiles a dataset.

        The database entry is identified by dataset_record."""

        for tile_type_id in dataset_record.list_tile_types():

            band_list = dataset_record.list_bands(tile_type_id)

            band_stack = dataset.stack_bands(band_list)

            for tile_footprint in dataset_record.get_coverage(tile_type_id):
                self.make_one_tile(dataset_record, tile_footprint, band_stack)

    def make_one_tile(self, dataset_record, tile_footprint, band_stack):
        """Makes a single tile."""

        tile_contents = self.collection.create_tile_contents(tile_footprint,
                                                             band_stack)

        if tile_contents.has_data():
            tile_record = dataset_record.create_tile_record(tile_footprint,
                                                            tile_contents)
            tile_record.make_mosaics()
        else:
            tile_contents.remove()

    #
    # Abstract methods
    #
    # These are abstract methods, designed to be overidden. They are
    # here to document what needs to be implemented in a subclass.
    #
    # The abstract method decorator checks that the abstract method
    # has be overridden when a subclass is *instantiated* rather than
    # when the method is called.
    #

    @abstractmethod
    def find_datasets(self, source_dir):
        """Return a list of path to the datasets under 'source_dir'.

        This is an abstract method since the method of identifying a
        dataset may vary between dataset formats.
        """

        raise NotImplementedError

    @abstractmethod
    def open_dataset(self, dataset_path):
        """Create and return a dataset object.

        dataset_path: points to the dataset to be opened and have
           its metadata read.

        Dataset objects differ for different types of dataset, but
        should present the same interface to the database classes. They
        contain the dataset path.
        """

        raise NotImplementedError

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


class DatasetError(Exception):
    """
    A problem specific to a dataset. If raised it will cause the
    current dataset to be skipped, but the ingest process will continue.
    """

    pass
