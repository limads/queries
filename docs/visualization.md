# Data visualization

Queries can be used to visualize and export data in simple 2D graphs using 
plain SQL syntax. Queries will render a plot any time a query return a single JSON object
(i.e. a table with a single row and single column) satisfying the papyri JSON schema. 
While you can generate the JSON using a combination of SQL literals combined with aggregate functions,
it is more practical and less error-prone to use a few user defined functions (UDFs)
that generate valid plot definitions. There are a few UDFs for such purpose 
described below (remember that you should create the functions via other
tool such as `psql`, since Queries does not currently support function creation).
Those functions are available at the script `docs/examples/plots.sql`.

Plots are composed from one or more mappings. Each mapping in a JSON object
described in the implementation of the mapping functions below. The first
arguments to each function is a Postgres `array[]` of a numeric type
(`real`, `double precision` or `integer`) with the data to be mapped.
The data can be easily aggregated with the `array_agg` builtin function.
The remaining arguments are specifications of how the mapping should look
like (color, trace width), and each have a default argument.

## Line mapping

```sql
create or replace function line(
    anyarray,
    anyarray,
    text default '#000000',
    real default 1.0, 
    integer default 1
) returns json as $$
	select json_build_object(
	    'kind', 'line', 
	    'map', json_build_object('x', $1, 'y', $2), 
	    'color', $3, 
	    'width', $4, 
	    'spacing', $5
	);
$$ language sql;
```

## Scatter mapping 

```sql
create or replace function scatter(
    anyarray,
    anyarray,
    text default '#d3d7cf',
    real default 10.0
) returns json as $$
	select json_build_object(
	    'kind', 'scatter', 
	    'map', json_build_object('x', $1, 'y', $2), 
	    'color', $3, 
	    'radius', $4
    );
$$ language sql;
```

## Bar mapping 

```sql
create or replace function bars(
    anyarray,
    text default '#000000',
    real default 0.0,
    real default 1.0,
    real default 1.0,
    boolean default 'f',
    boolean default 't'
) returns json as $$
	select json_build_object(
	    'kind', 'bar', 
	    'map', json_build_object('x', $1), 
	    'color', $2, 
	    'origin', $3,
	    'spacing', $4, 
	    'width', $5, 
	    'center', $6, 
	    'vertical', $7
	);
$$ language sql;
```

## Interval mapping

```sql
-- intervals have their center positioned as the first argument.
-- min and maximum are data units taken by the interval. vertical tells which is the data axis.
create or replace function intervals(
    anyarray,
    anyarray,
    anyarray,
    text default '#000000',
    real default 1.0,
    real default 1.0,
    real default 1.0,
    bool default 't'
) returns json as $$
	select json_build_object('kind', 'interval', 'map', json_build_object('x', $1, 'y', $2, 'z', $3), 'color', $4, 'width', $5, 'spacing', $6, 'limits', $7, 'vertical', $8);
$$ language sql;
```

## Label mapping

```sql
create or replace function labels(anyarray,anyarray,text[],text default '#000000',text default 'Monospace Regular 12') 
returns json as $$
	select json_build_object('kind', 'text', 'map', json_build_object('x', $1, 'y', $2, 'text', $3), 'color', $4, 'font', $5);
$$ language sql;
```

## Composing the plot

To compose a plot, assemble one or more mappings into an `array[]` of JSON objects, and use it as the first argument
to the following function.

```sql
create or replace function plot(
    json[] default array[]::json[],
    json default scale(),
    json default scale(),
    json default design(),
    json default layout()
) returns json as $$
	select json_build_object('mappings', $1, 'x', $2, 'y', $3, 'design', $4, 'layout', $5);
$$ language sql;
```

To edit the scales (variable labels, limits, logarithmic spacing, inversion) pass values to the
second argument (horizontal scale) and third argument (vertical scale).

```sql
create or replace function scale(
    text default '',
    real default 0.0,
    real default 1.0,
    text default 'tight',
    bool default 'f',
    bool default 'f',
    integer default 0,
    integer default 5,
    integer default 2
) returns json as $$
    select json_build_object(
        'label', $1,
        'from', $2,
        'to', $3,
        'adjust', $4,
        'log', $5,
        'invert', $6,
        'offset', $7,
        'intervals', $8,
        'precision', $9
    );
$$ language sql;
```

The design() and layout() passed as fourth and fifth arguments changes how the plot looks and the dimensions 
of the exported SVG or PNG file.

```sql
create or replace function layout(integer default 800,integer default 600,real default 0.5,real default 0.5,text default 'unique') 
returns json as $$
    select json_build_object('width', $1,'height', $2,'vratio', $3,'hratio', $4,'split', $5);
$$ language sql;

create or replace function design(
    text default '#ffffff',
    text default '#d3d7cf',
    integer default 1,
    text default 'Monospace Regular 22'
) returns json as $$
    select json_build_object('bgcolor', $1,'fgcolor', $2,'width', $3,'font', $4);
$$ language sql;
```

## Composing multiple plots 

To compose multiple plots, use the following function:

```sql
create or replace function panel(json[], json default design(), json default layout()) returns json as $$
    select json_build_object('plots', $1, 'design', $2, 'layout', $3);
$$ language sql;
```

Up to 4 plots can be arranged simultaneously using the panel(.)
call. The first argument is an array of up to 4 plots, while the
second is the layout() definition, and the third is a design()
definition. Any design and layout specifications for the panel overwrite 
plot-specific specifications.

# Examples

The examples below use the iris table, populated from the SQL script at `docs/examples/iris.sql`.

## Line + Scatter plot

```sql
with regression as (
    select generate_series(0.0, 10.0) as x, 
        4.3066 + 0.4089*generate_series(0.0, 10.0) as y
), preds as (
    select line(array_agg(x), array_agg(y)) from regression
), data as (
    select scatter(
        array_agg(petal_length), 
        array_agg(sepal_length),
        '#92b9d8',
        10.0
    ) from iris
) select plot(
    array[
        data.scatter,
        preds.line
    ],
    scale('Petal length'),
    scale('Sepal length')
) from preds cross join data;
```

![](https://limads.sfo3.cdn.digitaloceanspaces.com/queries-plots/plot1.svg)

## Bar + Interval + Label plot

```sql
with length_avgs as(
    select 
        species,
        avg(sepal_length) as avgs,
        avg(sepal_length) - stddev(sepal_length) as lim_mins,
        avg(sepal_length) + stddev(sepal_length) as lim_maxs,
        avg(sepal_length) + stddev(sepal_length) + 0.2 as lim_maxs_upper
    from iris group by species
) select plot(
    array[
        bars(
            array_agg(avgs),
            '#92b9d8'::text, 
            0.0::real, 
            1.0::real, 
            0.5::real, 
            'f', 
            't'
        ),
        
        intervals(
            array[0.25, 1.25, 2.25]::double precision[],
            array_agg(lim_mins),
            array_agg(lim_maxs),
            '#000000',
            1.0,
            1.0,
            0.1,
            't'
        ),
        
        labels(
            array[0.25, 1.25, 2.25]::double precision[],
            array_agg(lim_maxs_upper),
            array_agg(species),
            '#000000',
            'Liberation Sans 22'
        )
    ],
    
    scale('', 0.0, 3.0, 'tight', 'f', 'f', 0, 0),
    
    scale('Petal length'),
    
    design('#ffffff', '#d3d7cf', 1, 'Monospace Regular 22')
    
) from length_avgs;
```

![](https://limads.sfo3.cdn.digitaloceanspaces.com/queries-plots/plot2.svg)

# Real time graph updates

Combined with the query schedule feature, queries can be used to monitor 
database changes in real time from simple SQL scripts. The same
script can contain queries returning tables and plots, and queries will
arrange all the outputs in the same order the statements were called.

# Workflow

Since plot composition is done within aggregated queries, they can easily
be wrapped in a CREATE VIEW statement, which can in turn be called from menu-based
interactions. Any updates on the database tables will naturally reflect
on the composed plot.


