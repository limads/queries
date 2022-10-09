create or replace function line(anyarray,anyarray,text default '#000000',real default 1.0, integer default 1) 
returns json as $$
	select json_build_object('kind', 'line', 'map', json_build_object('x', $1, 'y', $2), 'color', $3, 'width', $4, 'spacing', $5);
$$ language sql;

create or replace function scatter(anyarray,anyarray,text default '#d3d7cf',real default 10.0) 
returns json as $$
	select json_build_object('kind', 'scatter', 'map', json_build_object('x', $1, 'y', $2), 'color', $3, 'radius', $4);
$$ language sql;

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

create or replace function labels(anyarray,anyarray,text[],text default '#000000',text default 'Monospace Regular 12') 
returns json as $$
	select json_build_object('kind', 'text', 'map', json_build_object('x', $1, 'y', $2, 'text', $3), 'color', $4, 'font', $5);
$$ language sql;

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

-- Create a plot scale. 
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

create or replace function plot(
    json[] default array[]::json[],
    json default scale(),
    json default scale(),
    json default design(),
    json default layout()
) returns json as $$
	select json_build_object('mappings', $1, 'x', $2, 'y', $3, 'design', $4, 'layout', $5);
$$ language sql;

create or replace function panel(json[], json default design(), json default layout()) returns json as $$
    select json_build_object('plots', $1, 'design', $2, 'layout', $3);
$$ language sql;

