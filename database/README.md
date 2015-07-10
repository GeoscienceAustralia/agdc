
# Database migrations

## Initial setup

You will first need to create a database and users:
    
    create group cube_admin_group;
    create group cube_user_group;

    create user cube_admin with password '<cube_admin_pass>' in group cube_admin_group;
    create user cube_user with password '<cube_user_pass>' in group cube_user_group;

    create database datacube encoding 'UTF-8' owner cube_admin;

Where:
- `datacube` is the default database name, but can be changed as needed. (in `datacube.conf` and `flyway.properties`)
- The two users and groups are required for the schema creation scripts. `cube_admin` will have ownership 
of all tables, and `cube_user` will have read-only access to data.

Once the database is created, initialise postgres extensions within it:

    psql -f setup-database-extensions.sql datacube

Now you can add the tables and schema.

## Schema tracking

The ``sql`` directory contains database change files.

Each change file is a plain sql file following simple naming conventions: ``v<number>__description.sql``

Example:

    sql/v1__schema.sql
    sql/v2__modis_information.sql

These files can be run directly via psql — or preferably using a tool such as Flyway — provided you
run them in versioned order.

## Flyway

Flyway is the recommended method of creating and managing agdc databases. It is a simple tool that tracks
 the changes applied to each database, and allows updates to be applied easily. Developers can also 
 use it on their own machines to easily create, update and drop databases as needed.

### Setup

- Download and extract the command-line package from the [Flyway website](http://flywaydb.org/).
- Copy ``flyway-example.properties`` into ``conf/flyway.properties`` within that folder.
- Edit `conf/flyway.properties` as needed. particularly:
    - The sql directory location (if not extracted to this current directory).
    - Database connection settings (if not localhost:datacube / cube_admin:cube_admin )

View database status:

    ./flyway info

Apply missing changes:

    ./flyway migrate

Flyway creates a table called `schema_version` in each database, which contains a record of each applied change,
including audit information such as user, date and script checksum.

When new change scripts are added to the repository in the future, running `./flyway migrate`
will update the database with missing changes.
