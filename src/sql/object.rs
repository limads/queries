use std::cmp::{Eq, PartialEq};
use std::str::FromStr;
use std::fmt;
use serde::{Serialize, Deserialize};

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, Hash)]
pub struct DBDetails {
    pub uptime : String,
    pub server : String,
    pub size : String,
    pub locale : String
}

#[derive(Debug, Clone, Default)]
pub struct DBInfo {
    pub schema : Vec<DBObject>,
    pub details : Option<DBDetails>
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum DBType {
    Bool,
    I16,
    I32,
    I64,
    F32,
    F64,
    Numeric,
    Text,
    Date,
    Time,
    Bytes,
    Json,
    Xml,
    Array,
    Trigger,
    Unknown
}

impl FromStr for DBType {

    type Err = ();

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        // The underline prefix to array types is set by the return of proargtypes join typename,
        // the bracket postfix is the return of pg_get_function_identity_arguments.
        if s.starts_with("_") || s.ends_with("[]") {
            return Ok(Self::Array)
        }
        match s {
            "boolean" | "bool" | "BOOL" => Ok(Self::Bool),
            "bigint" | "bigserial" | "int8" => Ok(Self::I64),
            "bit" | "bit varying" | "character" | "character varying" | "text" | "name" | "char" | "cstring" => Ok(Self::Text),
            "date" | "DATE" => Ok(Self::Date),
            "json" | "jsonb" | "record" => Ok(Self::Json),
            "numeric" => Ok(Self::Numeric),
            "integer" | "int" | "INTEGER" | "INT" | "int4" => Ok(Self::I32),
            "smallint" | "smallserial" | "int2" => Ok(Self::I16),
            "real" | "REAL" | "float4" => Ok(Self::F32),
            "dp" | "double precision" | "float8" => Ok(Self::F64),
            "blob" | "BLOB" | "bytea" => Ok(Self::Bytes),
            "time" | "time with time zone" | "time without time zone" |
            "timestamp with time zone" | "timestamp without time zone" | "time with time zone" | "time without time zone" => Ok(Self::Time),
            "xml" => Ok(Self::Xml),
            "anyarray" | "array" | "ARRAY" => Ok(Self::Array),
            "trigger" => Ok(Self::Trigger),
            _ => Ok(Self::Unknown)
        }
    }

}

impl fmt::Display for DBType {

    fn fmt(&self, f : &mut fmt::Formatter) -> fmt::Result {
        let name = match self {
            Self::Bool => "bool",
            Self::I16 => "smallint",
            Self::I32 => "integer",
            Self::I64 => "bigint",
            Self::F32 => "real",
            Self::F64 => "dp",
            Self::Numeric => "numeric",
            Self::Text => "text",
            Self::Date => "date",
            Self::Time => "time",
            Self::Bytes => "bytea",
            Self::Json => "json",
            Self::Xml => "xml",
            Self::Array => "array",
            Self::Trigger => "trigger",
            Self::Unknown => "unknown"
        };
        write!(f, "{}", name)
    }

}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Relation {
    pub tgt_schema : String,
    pub tgt_tbl : String,
    pub src_col : String,
    pub tgt_col : String
}

/*#[derive(Debug, Clone)]
pub enum DBView {

    Schema { name : String, children : Vec<DBView> },

    View { name : String }
}

#[derive(Debug, Clone)]
pub enum DBFunction {

    Schema { name : String, chidren : Vec<DBFunction> },

    Function { args : Vec<DBType>, ret : DBType }

}*/

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum DBObject {

    // In practice, children will always hold table variants.
    Schema{ name : String, children : Vec<DBObject> },

    Table{ schema : String, name : String, cols : Vec<(String, DBType, bool)>, rels : Vec<Relation> },

    Function { schema : String, name : String, args : Vec<DBType>, arg_names : Option<Vec<String>>, ret : Option<DBType> },

    View { schema : String, name : String }

}

impl DBObject {

    pub fn obj_name(&self) -> &str {
        match &self {
            Self::Schema { name, .. } => &name[..],
            Self::Table { name, .. } => &name[..],
            Self::Function { name, .. } => &name[..],
            Self::View { name, .. } => &name[..]
        }
    }

    /// Gets a table or schema by recursively indexing this structure.
    pub fn get_table_or_schema(&self, sub_ixs : &[usize]) -> Option<DBObject> {
        if sub_ixs.len() == 1 {
            match &self {
                DBObject::Schema{ children, .. } => {
                    children.get(sub_ixs[0]).cloned()
                },
                DBObject::Table{ .. } => None,
                _ => None
            }
        } else {
            match &self {
                DBObject::Schema{ children, .. } => {
                    children.get(sub_ixs[0])?.get_table_or_schema(&sub_ixs[1..])
                },
                DBObject::Table { .. } => None,
                _ => None
            }
        }
    }
}

pub fn index_db_object(objs : &[DBObject], ixs : Vec<usize>) -> Option<DBObject> {
    if let Some(root_obj) = objs.get(ixs[0]).cloned() {
        if ixs.len() == 1 {
            Some(root_obj)
        } else {
            root_obj.get_table_or_schema(&ixs[1..])
        }
    } else {
        None
    }
}

/// Verify if table name exist on top-level or public schema.
pub fn schema_has_table(table : &str, schema : &[DBObject]) -> bool {
    for obj in schema.iter() {
        match obj {
            DBObject::Schema { ref name, ref children } => {
                if name == "public" {
                    return schema_has_table(table, &children[..]);
                }
            },
            DBObject::Table { ref name, .. } => {
                if name == table {
                    return true;
                }
            },
            _ => {  }
        }
    }
    false
}

pub fn build_er_diagram(mut er : String, schemata : &[DBObject]) -> String {
    for obj in schemata.iter() {
        match &obj {
            DBObject::Schema { children, .. } => {
                er = build_er_diagram(er, children);
            },
            DBObject::Table { schema, name, cols, rels } => {
                let cols : String = cols.iter().map(|c| c.0.clone() ).collect::<Vec<_>>().join("\\n");
                let mut tbl = format!("{} [ label = \"{} | {} \"];\n", name, name, cols);
                for rel in rels.iter() {
                    tbl += &format!("{} -- {} [label=\"1:n\"];\n", rel.tgt_tbl, name);
                }
                er += &tbl[..];
            },
            _ => { }
        }
    }
    er
}

impl fmt::Display for DBObject {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let name : &str = match &self {
            DBObject::Schema{ name, .. } => &name,
            DBObject::Table{ name, ..} => &name,
            DBObject::Function{ name, ..} => &name,
            DBObject::View{ name, ..} => &name
        };
        write!(f, "{}", name)
    }
}

