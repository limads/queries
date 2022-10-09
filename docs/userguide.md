# Queries user guide

# Connecting to your database

While Queries is being designed to be agnostic to database engine, the current implementation
only supports connections to PostgreSQL database clusters. Two kinds of connections 
are currently supported:

- Local, non-encrypted.

- Remote, encrypted via TLS or SSL.

## Local connections

To connect to a local database, use either localhost:5432 or 127.0.0.1:5432 as the Host field.
If you haven't configured any users or passwords, usually Postgres sets the current Unix user name
as the username and also as the password.

## Remote connections

Any remote connections (i.e. not to localhost) should use either SSL or TLS encryption. Queries does not currently support 
connecting to LAN network databases or remote servers without encryption. That means you must point to
a valid CA certificate path before attempting to connect. 

To add a certificate, go to the Security settings, click the Certificates
option, inform the Host:Port field and a matching certificate path (this should be a local path to a certificate
file, usually with the extension .crt or .pem), and click the "plus" button to the left to add the certificate.
If you are using a cloud service, this file should have been sent by your provider. Make sure you do not inform 
duplicate certificate paths to the same Host. 

You can remove added certificates via the "minus" button that appears when you hover you cursor over added certificates.

## Troubleshooting

1. Make sure you can connect to the database via the `psql` command-line tool first. If you cannot,
then the problem is likely with your connection. If you are using a cloud service, make
sure your IP is authorized to make connections to the database.

2. If you can connect to your database via `psql`, verify if are using the authentication
fields correctly. The first field must follow the format Host:Port, where host is the equivalent
to the `-h` or `--host` parameter to psql, and Port is the equivalent to the `-p` or `--port` parameter
to `psql`. User is the equivalent to the `-U` or `--username` to `psql`, and Password is the equivalent
to what you inform on the password prompt or via the `PGPASSWORD` environment variable.

3. Double check if the path to your certificate is valid (see the Remote Connections section above).

If your connection is still being refused by Queries, you can open an issue at https://github.com/limads/queries/issues.

# Running SQL statements

## SQL standard compliance

Queries executes only ANSI-SQL (2011) compliant SQL scripts. This means that
Postgres-specific syntax extensions such as CREATE FUNCTION, CREATE TYPE, certain clauses
for table creation (GENERATED AS, PARTITION BY, PARTITION OF), certain transaction commands
(SAVEPOINT), on conflict clauses for INSERT statements, dollar-quoted strings, custom operator
syntax (among others Postgres-specific features), are currently unsupported. That
means Queries will refuse to execute some scripts that should be accepted by `psql`, for example.

## Script-based interaction

SQL scripts are executed by pressing the execute button at the title bar (the one with a database
and arrow icon), or by pressing the F7 shortcut whenever a script is selected on the left sidebar list. 
Query results are then presented in the queries workspace respecting the statement order in the script
(although those statements might actually be executed asynchronously if Queries determines their 
execution order cannot change the output).

By default, queries will reject any scripts containing potential destructive data modification 
statements (UPDATE, DELETE) or data definition statements (DROP, TRUNCATE, ALTER). This
can be changed in the settings at any time. Queries does not make any guarantees of
preventing indirect changes however (i.e. using a SELECT to call user-defined functions that 
wrap destructive statements in their implementations, or changes made via triggers).
Although the defaults are meant as an extra safeguard, you should treat Queries
with the same degree of caution as you would with any other application having
full access to your database.

## Menu-based interaction

Queries supports currently selecting tables and views by right-clicking
the database object of the left sidebar and selecting the "Query" menu item. 
Records are returned in an unspecified order and limited by the maximum number
of rows chosen at the settings. You can also use this menu to insert records 
individually (Insert menu item) or in batch mode from a CSV file (Import menu item).

## Automatic SQL execution

Some of Queries features rely on execution of automatically generated SQL 
in the situations described below. Queries by design will never
generate a statement that could lead to data loss as a result of
the statement semantics. But relational databases by their nature
allow side effects via triggers, so at all times be mindful those might apply
when you perform both script-based and menu-based interactions.

Queries will execute auto-generated SQL:

1. Immediately after every successful connection: Queries will issue a series of
SELECT statements to inspect the catalog tables under
the `pg_catalog` namespace to populate the left sidebar with your database
schemata, tables, views and functions. Queries will also issue a series of
SELECT and SHOW statements to retrieve contextual information about the 
database (uptime, size, etc). This will only be possible, however,
if the user connecting to the database has access to the respective
tables.

2. When the user clicks the Query/Report/Insert/Import menu items at the menu activated 
by a right-click over the schema tree, Queries will automatically generate the required SQL to 
complete the desired interaction. The Query and Report menu items generate a SELECT
statement to complete the action; The Insert and Import menu items generate an INSERT
statement to complete the action. Any triggers associated with those actions 
might lead to side effects.

## Scheduled query execution

Queries support monitoring database changes in real time by setting the scheduled
execution mode. In this mode, the currently-selected SQL script is executed
repeatedly at evenly-spaced intervals. You can change the execution interval
at the execution settings (from a minimum of 1 second up to 30 seconds). 
This mode is meant to execute scripts that contains exclusively query
statements (executing any data definition or modification statements
such as CREATE, INSERT, UPDATE and DELETE in scheduled mode is unsupported).

To activate scheduled execution mode, expand the drop-down menu located
together with the execution button at the header bar, and move the selection
from "Immediate" to "Scheduled". The next time you click the execution button,
the issued statement sequence will be repeatedly sent to the database until
the execution button is clicked again, or any errors are found.

## Exporting data

The results of any successful queries can be exported as CSV files by selecting the corresponding
table and clicking the "Export" button on the main menu. While the table size you see in
the workspace is limited by the maximum number of rows setting, the exported CSV files
always contain the full query output. Any queries that result in data visualizations
(see below) can be exported to either SVG or PNG files via the same button.

# Data visualization

Queries can be used to visualize and export data in simple 2D graphs using 
plain SQL syntax. Queries will render a plot any time a query return a single JSON object
(i.e. a table with a single row and single column) satisfying the papyri JSON schema. 
While you can generate the JSON using a combination of SQL literals combined with aggregate functions,
it is more practical and less error-prone to use a few user defined functions (UDFs)
that generate valid plot definitions. There are a few UDFs for such purpose 
described below (remember that you should create the functions via other
tool such as `psql`).

```sql

```

The most minimal plot is done by calling the plot with default arguments:

```sql
plot()
```

The first argument to the plot function is an array of JSON objects, 
where each object is a separate mapping. Each mapping in this array in turn, 
can be created by the functions line(.), scatter(.) and bar(.). Those functions
take arrays with the data to be plotted at the first arguments, and extra
graphical properties at the remaining arguments.

```sql
select plot(array[line(array[1,2,3]]);
```

It is common to work with the array_agg builtin to generate the required
data from a table:

```sql
select plot(line(array_agg(age), array_agg(bp))) from patients;
```

## Line plots 

## Bar plots

## Label plots

## Interval plots 

## Scatter plots

## Panels

Up to 4 plots can be arranged simultaneously using the panel(.)
call. The first argument is an array of up to 4 plots, while the
second is the layout() definition, and the third is a design()
definition.

## Real time graph updates

Combined with the query schedule feature, queries can be used to monitor 
database changes in real time from simple SQL scripts. The same
script can contain queries returning tables and plots, and queries will
arrange all the outputs in the same order the statements were called.

# Report generation

Queries can be used for dynamic report generation in the HTML format
by following those steps:

1. Save your HTML report template under ~/Templates. The template
should follow the rules described at the section below.

2. Chose a table or view that will yield the report contents.
Click under the table or view at the schema tree on the left
sidebar and choose the "Report" item.

For each row this table returns, the unique <section> tag on
your template will be replicated, but edited to contain the table contents according
to the rules described at the previous step.

3. Chose a destination to save the resulting HTML file.

## Template HTML files

Templates for dynamic report generation are HTML files with a <body></body> and a
single <section></section> nested within this body. 

Any HTML inside the <body> but outside the unique <section> tag will appear
once in the resulting report. Any HTML inside the <section>, however,
will be replicated for each row resulting
from your query. To actually substitute the HTML with the query result, use
`html <template>COLUMN_NAME</template>` Where <template> is the standard HTML 
template tag, and COLUMN_NAME is one of the columns returned by your query.

Apart from JSON objects, the template tags will be substituted for any textual
or numeric content resulting from the corresponding column in the query.
Null values will be replaced by the value specified on the
report generation dialog.

Any JSON fields that satisfy the papyri schema will be substituted by an SVG tag
containing the full plot embedded in the file.

JSON fields not satisfying the schema will be rendered
as HTML <table> tags if possible. JSON fields meant to be presented
as tables must map each key of its keys to a JSON array. 
The arrays might have heterogeneous types, but
are expected to be of same length. If the "Transpose" switch is off, 
each array will map to a different column, and the JSON keys
are mapped to the table header. If the switch is on, then each array will map
to a different row, the JSON keys will be mapped to the first column, 
and the table will have an empty header.

# Data security

Queries never saves database passwords to disk for security reasons. Other
authentication information, however, such as the host, user and database
name are persisted to disk at the default Flatpak user data directory. 
More cautious users might wish to disable that at the security settings window, 
at the cost of having to re-type the connection information and setup any
certificate paths for every session. 

When you uninstall Queries, it is highly recommended to erase all
user data as well, using:

```
flatpak uninstall com.github.limads.Queries --delete-data
```

So that no authentication information is persisted to disk after
the application is uninstalled.

Queries does not save any database content to disk, except what
is explicitly exported by the user.


