
# Database migrations

The ``sql`` directory contains database change files.

Each change file is a plain sql file following simple naming conventions: ``v<number>__description.sql``

Example:

    sql/v1__schema.sql
    sql/v2__modis_information.sql

These files can be run directly via psql — or preferably using a tool such as Flyway — provided you
run them in versioned order.

## Flyway

Flyway is the recommended method of creating and managing agdc databases. It is a simple tool that tracks
 the changes applied to each database.

### Setup

- Download and extract the command-line package from the [Flyway website](http://flywaydb.org/).
- Copy ``flyway-example.properties`` into ``conf/flyway.properties`` within that folder.
- Edit `conf/flyway.properties` as needed. particularly:
    - The sql directory location (if not extracted to this current directory).
    - Database connection settings (if not localhost / cube_admin:cube_admin)

View database status:

    ./flyway info

Apply missing changes:

    ./flyway migrate

Flyway creates a table called `schema_version` in each database, which contains a record of each applied change,
including audit information such as user, date and script checksum.