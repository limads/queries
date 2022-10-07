use filecase::*;
use queries::client::*;
use gtk4::glib;
mod common;
use std::thread;
use queries::sql::StatementOutput;
use std::rc::Rc;
use std::cell::RefCell;

// Double dolar sign string literals are unsupported syntax (they get mixed with SQL placeholders).

mod unsupported {

    const CREATE_TABLE : &'static str = r#"

    CREATE TABLE people (
        height_cm numeric,
        height_in numeric GENERATED ALWAYS AS (height_cm / 2.54) STORED
    );

    CREATE TABLE circles (
        c circle,
        EXCLUDE USING gist (c WITH &&)
    );

    CREATE TABLE capitals (
        state           char(2)
    ) INHERITS (cities);

    CREATE TABLE measurement (
        city_id         int not null,
        logdate         date not null,
        peaktemp        int,
        unitsales       int
    ) PARTITION BY RANGE (logdate);

    CREATE TABLE measurement_y2006m02 PARTITION OF measurement
        FOR VALUES FROM ('2006-02-01') TO ('2006-03-01');

    CREATE TABLE measurement_y2006m03 PARTITION OF measurement
        FOR VALUES FROM ('2006-03-01') TO ('2006-04-01');
        
    CREATE TABLE measurement_y2007m12 PARTITION OF measurement
        FOR VALUES FROM ('2007-12-01') TO ('2008-01-01')
        TABLESPACE fasttablespace;

    CREATE TABLE measurement_y2008m01 PARTITION OF measurement
        FOR VALUES FROM ('2008-01-01') TO ('2008-02-01')
        WITH (parallel_workers = 4)
        TABLESPACE fasttablespace;
    "#;
    
    const INSERT : &'static str = r#"
    INSERT INTO products (product_no, name, price) VALUES (1, 'Cheese', DEFAULT);
    INSERT INTO products DEFAULT VALUES;
    INSERT INTO users (firstname, lastname) VALUES ('Joe', 'Cool') RETURNING id;
    "#;
    
    const ALTER : &'static str = r#"
        ALTER TABLE measurement_y2006m02 NO INHERIT measurement;
        ALTER TABLE accounts ENABLE ROW LEVEL SECURITY;
        ALTER TABLE measurement DETACH PARTITION measurement_y2006m02;
        ALTER TABLE measurement DETACH PARTITION measurement_y2006m02 CONCURRENTLY;
        ALTER TABLE measurement_y2006m02 ADD UNIQUE (city_id, logdate);
    "#;
    
    const TRANSACTION : &'static str = r#"
    BEGIN;
    UPDATE products SET price = 10 WHERE price = 5;
    SAVEPOINT my_savepoint;
    UPDATE products SET price = 7 where price = 10;
    ROLLBACK TO my_savepoint;
    UPDATE products SET price = 8 where price = 7;
    COMMIT;
    "#;
    
}

// The SQL examples are taken from the chapters 4-7 of 
// the PostgreSQL docs, which is a good way to ensure they are representative
// and reasonably comprehensive.
const TABLE_CREATION : &'static str = r#"
CREATE TABLE products (
    product_no integer,
    name text,
    price numeric DEFAULT 9.99 CONSTRAINT positive_price CHECK (price > 0)
);

CREATE TABLE products2 (
    product_no integer,
    name text,
    price numeric,
    CHECK (price > 0),
    discounted_price numeric,
    CHECK (discounted_price > 0),
    CHECK (price > discounted_price)
);

CREATE TABLE products3 (
    product_no integer,
    name text,
    price numeric,
    CHECK (price > 0),
    discounted_price numeric,
    CHECK (discounted_price > 0),
    CONSTRAINT valid_discount CHECK (price > discounted_price)
);

CREATE TABLE example (
    a integer,
    b integer,
    c integer,
    UNIQUE (a, c)
);

create table other_table(c1 integer, c2 integer, unique(c1, c2));

CREATE TABLE testf (
  a integer PRIMARY KEY,
  b integer,
  c integer,
  FOREIGN KEY (b, c) REFERENCES other_table (c1, c2)
);

CREATE TABLE accounts (manager text, company text, contact_email text);

CREATE TABLE users (firstname text, lastname text, id serial primary key);

CREATE TABLE cities (
    name            text,
    population      float,
    elevation       int     
);

CREATE TABLE foo (fooid integer, foosubid integer, fooname text);

CREATE TABLE t1(num int, name text);
CREATE TABLE t2(num int, value text);

create table test1(x text, y integer);
create table items_sold(brand text, size text, sales integer);

create table orders(region text, amount integer, quantity integer, product text);
create table regional_sales(total_sales integer);

create table new_products(product_no integer, name text, price numeric(2), release_date text);

create table mytable(a integer, b integer, c integer, col1 integer);

create table measurement(city_id integer, logdate date);
"#;

const CREATE_INDEX : &'static str = r#"
CREATE INDEX measurement_usls_idx ON ONLY measurement (unitsales);

CREATE INDEX measurement_usls_200602_idx
    ON measurement_y2006m02 (unitsales);
    
"#;

const CREATE_FUNC : &'static str = r#"

CREATE FUNCTION getfoo(int) RETURNS SETOF foo AS '
    SELECT * FROM foo WHERE fooid = $1;
' LANGUAGE SQL;

CREATE OR REPLACE FUNCTION measurement_insert_trigger()
RETURNS TRIGGER AS '
BEGIN
    INSERT INTO measurement_y2008m01 VALUES (NEW.*);
    RETURN NULL;
END;
' LANGUAGE plpgsql;
"#;

const CREATE_TRIGGER : &'static str = r#"
CREATE TRIGGER insert_measurement_trigger
    BEFORE INSERT ON measurement
    FOR EACH ROW EXECUTE FUNCTION measurement_insert_trigger();
"#;

const CREATE_RULE : &'static str = r#"
CREATE RULE measurement_insert_y2006m02 AS
ON INSERT TO measurement WHERE
    ( logdate >= DATE '2006-02-01' AND logdate < DATE '2006-03-01' )
DO INSTEAD
    INSERT INTO measurement_y2006m02 VALUES (NEW.*);
"#;

const CREATE_TYPE : &'static str = r#"
CREATE TYPE rainbow AS ENUM ('red', 'orange', 'yellow',
                             'green', 'blue', 'purple');
"#;

const INDEX_CREATION : &'static str = r#"
CREATE INDEX ON measurement (logdate);
"#;

const INDEX_ALT : &'static str = r#"
ALTER INDEX measurement_usls_idx ATTACH PARTITION measurement_usls_200602_idx;

ALTER INDEX measurement_city_id_logdate_key
    ATTACH PARTITION measurement_y2006m02_city_id_logdate_key;
"#;

const SCHEMA_CREATION : &'static str = r#"
CREATE SCHEMA myschema;
"#;

const INSERTION : &'static str = r#"
INSERT INTO products VALUES (1, 'Cheese', 9.99);
INSERT INTO products (product_no, name, price) VALUES (1, 'Cheese', 9.99);
INSERT INTO products (product_no, name, price) VALUES
    (1, 'Cheese', 9.99),
    (2, 'Bread', 1.99),
    (3, 'Milk', 2.99);

INSERT INTO t1 VALUES (1, 'a'), (2, 'b'), (3, 'c');
INSERT INTO t2 VALUES (1, 'xxx'), (3, 'yyy'), (5, 'zzz');
INSERT INTO test1 VALUES ('a', 3),('c', 2),('b', 5),('a', 1);
INSERT INTO items_sold VALUES ('Foo', 'L', 10),('Foo', 'M', 20),('Bar', 'M', 15),('Bar', 'L', 5);

INSERT INTO products (product_no, name, price)
  SELECT product_no, name, price FROM new_products
    WHERE release_date = 'today';
"#;

const UPDATE : &'static str = r#"
UPDATE products SET price = 10 WHERE price = 5;
UPDATE products SET price = price * 1.10;
UPDATE mytable SET a = 5, b = 3, c = 1 WHERE a > 0;

BEGIN;
UPDATE products SET price = 10 WHERE price = 5;
UPDATE products SET price = 7 where price = 10;
UPDATE products SET price = 8 where price = 7;
COMMIT;
"#;

const DELETE : &'static str = r#"
DELETE FROM products WHERE price = 10;
DELETE FROM products;
"#;

fn privileges() -> String {
    let user = common::run("whoami").unwrap().trim().to_string();
    format!(
        r#"
        GRANT UPDATE ON products TO {};
        REVOKE ALL ON products FROM PUBLIC;
        GRANT SELECT ON mytable TO PUBLIC;
        GRANT SELECT, UPDATE, INSERT ON mytable TO {};
        GRANT SELECT (col1), UPDATE (col1) ON mytable TO {};
        "#, 
        user, 
        user, 
        user
    )
}

const POLICY : &'static str = r#"
CREATE POLICY account_managers ON accounts TO managers
    USING (manager = current_user);
    
CREATE POLICY user_policy ON users
    USING (user_name = current_user);
    
CREATE POLICY user_sel_policy ON users
    FOR SELECT
    USING (true);
CREATE POLICY user_mod_policy ON users
    USING (user_name = current_user);
"#;

const CREATE_VIEW : &'static str = r#"
CREATE VIEW vw_foo AS SELECT * FROM foo;
-- CREATE VIEW vw_getfoo AS SELECT * FROM getfoo(1);
"#;

const TABLE_ALT : &'static str = r#"
ALTER TABLE products ADD CONSTRAINT some_name check(not(name is null));
ALTER TABLE products ADD COLUMN description text;
ALTER TABLE products ADD COLUMN description2 text CHECK (description <> '');
ALTER TABLE products DROP COLUMN description CASCADE;
ALTER TABLE products DROP COLUMN description2 CASCADE;
ALTER TABLE products DROP CONSTRAINT some_name;
ALTER TABLE products ALTER COLUMN price SET DEFAULT 7.77;
ALTER TABLE products ALTER COLUMN price DROP DEFAULT;
ALTER TABLE products ALTER COLUMN price TYPE numeric(10,2);
ALTER TABLE products RENAME COLUMN product_no TO product_number;
ALTER TABLE products RENAME TO items;
ALTER TABLE items RENAME TO products;
ALTER TABLE ONLY measurement ADD UNIQUE (city_id, logdate);
"#;

const QUERIES : &'static str = r#"

SELECT random();

SELECT * FROM t1 CROSS JOIN t2;

SELECT * FROM t1 INNER JOIN t2 ON t1.num = t2.num;

SELECT * FROM t1 INNER JOIN t2 USING (num);

SELECT * FROM t1 NATURAL INNER JOIN t2;

SELECT * FROM t1 LEFT JOIN t2 ON t1.num = t2.num;

SELECT * FROM t1 LEFT JOIN t2 USING (num);

SELECT * FROM t1 RIGHT JOIN t2 ON t1.num = t2.num;

SELECT * FROM t1 FULL JOIN t2 ON t1.num = t2.num;

SELECT * FROM t1 LEFT JOIN t2 ON t1.num = t2.num AND t2.value = 'xxx';

SELECT * FROM t1 LEFT JOIN t2 ON t1.num = t2.num WHERE t2.value = 'xxx';

-- Only when getfoo is defined.
-- SELECT * FROM foo
-- WHERE foosubid IN (
--    SELECT foosubid
--    FROM getfoo(foo.fooid) z
--    WHERE z.fooid = foo.fooid
-- );
-- SELECT * FROM vw_getfoo;

/*SELECT *
FROM ROWS FROM
    (
        json_to_recordset('[{"a":40,"b":"foo"},{"a":"100","b":"bar"}]')
            AS (a INTEGER, b TEXT),
        generate_series(1, 3)
    ) AS x (p, q, s)
ORDER BY p;*/

SELECT x FROM test1 GROUP BY x;
SELECT x, sum(y) FROM test1 GROUP BY x;
SELECT x, sum(y) FROM test1 GROUP BY x HAVING sum(y) > 3;
SELECT x, sum(y) FROM test1 GROUP BY x HAVING x < 'c';
SELECT brand, size, sum(sales) FROM items_sold GROUP BY GROUPING SETS ((brand), (size), ());

WITH regional_sales AS (
    SELECT region, SUM(amount) AS total_sales
    FROM orders
    GROUP BY region
), top_regions AS (
    SELECT region
    FROM regional_sales
    WHERE total_sales > (SELECT SUM(total_sales)/10 FROM regional_sales)
)
SELECT region,
       product,
       SUM(quantity) AS product_units,
       SUM(amount) AS product_sales
FROM orders
WHERE region IN (SELECT region FROM top_regions)
GROUP BY region, product;

WITH RECURSIVE t(n) AS (
    VALUES (1)
  UNION ALL
    SELECT n+1 FROM t WHERE n < 100
)
SELECT sum(n) FROM t;

"#;

const TABLE_DROP : &'static str = r#"
DROP TABLE users;
DROP TABLE products;
DROP TABLE products2;
DROP TABLE products3;
DROP TABLE example;
DROP TABLE t1;
DROP TABLE accounts;
DROP TABLE cities;
DROP TABLE measurement;
"#;

const SCHEMA_DROP : &'static str = r#"
DROP SCHEMA myschema;
"#;

fn exec_next_statement(stmt_ix : &Rc<RefCell<usize>>, all_stmts : &[String], sender : &glib::Sender<ActiveConnectionAction>) {
    let mut stmt_ix = stmt_ix.borrow_mut();
    *stmt_ix += 1; 
    if *stmt_ix < all_stmts.len() {
        sender.send(ActiveConnectionAction::ExecutionRequest(all_stmts[*stmt_ix].to_string())).unwrap();
    } else {
        println!("All statements executed");
    }
}

// cargo test -- execution --nocapture
#[test]
pub fn execution() {

    /* This test should iterate over a vector of statements by sending an ExecutionRequest message
    immediately after connection and after any results arrive. Panics on any errors. */
    common::run_with_temp_db(|temp| {
        gtk4::init();
        let stmt_ix = Rc::new(RefCell::new(0));
        
        let user_state = SharedUserState::default();
        {
            let mut us = user_state.borrow_mut();
            us.execution.accept_dml = true;
            us.execution.accept_ddl = true;
        }
        
        let privs = privileges();
        let all_stmts = [
            TABLE_CREATION.to_string(),
            // CREATE_FUNC.to_string(), 
            // CREATE_TRIGGER.to_string(), 
            // CREATE_RULE.to_string(), 
            // CREATE_TYPE.to_string(), 
            CREATE_VIEW.to_string(),
            // POLICY.to_string(), 
            // INDEX_CREATION.to_string(), 
            // INDEX_ALT.to_string(), 
            SCHEMA_CREATION.to_string(), 
            INSERTION.to_string(), 
            UPDATE.to_string(), 
            DELETE.to_string(), 
            QUERIES.to_string(),
            TABLE_ALT.to_string(), 
            privs.to_string(),
            TABLE_DROP.to_string(), 
            SCHEMA_DROP.to_string(),
        ];
        
        let conn = ActiveConnection::new(&user_state);
        conn.connect_db_connected({
            let sender = conn.sender().clone();
            move|(conn,info)| {
                println!("Conneted to {:?}", conn);
                println!("Database info: {:?}", info);
                sender.send(ActiveConnectionAction::ExecutionRequest(TABLE_CREATION.to_string())).unwrap();
            }
        });
        conn.connect_db_conn_failure(move |e| {
            panic!("{:?}", e);
        });
        conn.connect_db_disconnected(move|_| {
            panic!("Database disconnected");
        });
        conn.connect_db_error(|e| {
            panic!("{}", e);
        });
        conn.connect_schema_update({
            let stmt_ix = stmt_ix.clone(); 
            let sender = conn.sender().clone();
            let all_stmts = all_stmts.clone();
            move |update| {
                // Called after create table/create view statements
                exec_next_statement(&stmt_ix, &all_stmts[..], &sender);
            }
        });
        conn.connect_exec_result({
            let sender = conn.sender().clone();
            let stmt_ix = stmt_ix.clone();
            move |res| {
                for r in &res {
                    match r {
                        StatementOutput::Invalid(msg, by_engine) => {
                            if *by_engine {
                                println!("Statement rejected by server: {}", msg);
                            } else {
                                println!("Statement rejected by client: {}", msg);
                            }                            
                        },
                        out => { 
                            println!("Statement executed: {:?}", out);
                        }
                    }
                }
                
                // If there is at least one create/alter, that means the next call will happen at the schema update callback.
                let changed_schema = res.iter().any(|o| {
                    match o {
                        StatementOutput::Modification(s) => {
                            s.starts_with("Create") || s.starts_with("Alter") || s.starts_with("Drop")
                        },
                        _ => {
                            false
                        }
                    }
                });
                if !changed_schema {
                    exec_next_statement(&stmt_ix, &all_stmts[..], &sender);
                }
            }
        });
        
        conn.send(ActiveConnectionAction::ConnectRequest(temp.uri()));
        println!("Running...");
        common::run_loop_for_ms(10_000);
        println!("Done");
        
    });
}

