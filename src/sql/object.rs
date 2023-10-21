/*Copyright (c) 2022 Diego da Silva Lima. All rights reserved.

This work is licensed under the terms of the GPL v3.0 License.  
For a copy, see http://www.gnu.org/licenses.*/

use std::cmp::{Eq, PartialEq};
use std::str::FromStr;
use std::fmt;
use serde::{Serialize, Deserialize};
use crate::ui::model::ModelDesign;

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

impl DBInfo {

    /* Must use a mono font, or else spacing will be uneven across lines. */
    pub fn diagram(&self, design : &ModelDesign) -> String {
        let mut s = String::new();
        let mut all_tbls = Vec::new();
        collect_tbls(&mut all_tbls, &self.schema);
        s = build_er_diagram(s, &self.schema, &all_tbls);
        let ModelDesign { background, node_fill, font_name, font_size, font_color } = design;
        format!(r##"
            graph {{
                ratio=1.6;
                dpi=96;
                bgcolor="{background}";
                rankdir="LR";
                node [
                    fontname = "{font_name}",
                    fontcolor = "{font_color}",
                    style="filled",
                    color="#bbbbbb",
                    fillcolor="{node_fill}",
                    fontsize={font_size},
                    margin=0.12
                ];
                edge [
                    fontname = "{font_name}",
                    color="#bbbbbb",
                    fontcolor = "{font_color}",
                    fontsize=12.0
                ];
                {s}
            }}
        "##)
    }

}

fn collect_tbls(names : &mut Vec<(String, String)>, tbls : &[DBObject]) {
    for t in tbls {
        match t {
            DBObject::Schema { children, .. } => {
                collect_tbls(names, children);
            },
            DBObject::Table { schema, name, .. } => {
                names.push((schema.clone(), name.clone()));
            },
            _ => {

            }
        }
    }
}

fn cols_string(cols : &[DBColumn]) -> String {
    if cols.len() == 0 {
        return String::new();
    }
    let lens : Vec<usize> = cols.iter().map(line_len).collect();
    let largest_line = lens.iter().max().unwrap();
    let mut lines = Vec::new();
    for i in 0..cols.len() {
        let extra_space = largest_line - lens[i];
        let mut s = cols[i].name.clone();
        for i in 0..(MIN_SPACE+extra_space) {
            s.push(' ');
        }
        s += &cols[i].ty.name();
        lines.push(s);
    }
    lines.join("<br/><br/>")
}

const MIN_SPACE : usize = 8;

fn line_len(col : &DBColumn) -> usize {
    col.name.len() + col.ty.name().len() + MIN_SPACE
}

// penwidth for link thickness.
pub fn build_er_diagram(mut er : String, schemata : &[DBObject], all_tbls : &[(String,String)]) -> String {
    // let mut disconnected = Vec::new();
    for obj in schemata.iter() {
        match &obj {
            DBObject::Schema { children, .. } => {
                er = build_er_diagram(er, children, all_tbls);
            },
            DBObject::Table { schema, name, cols, rels } => {
                let cols = cols_string(&cols);
                let qual_name = if schema == crate::server::PG_PUB || schema.is_empty() {
                    name.clone()
                } else {
                    format!("{}.{}", schema, name)
                };

                let lbl = format!("<b>{qual_name}</b><br/><br/>{cols}");
                let mut tbl = format!("{name} [ label = <{lbl}>, shape=\"note\"];\n");
                for rel in rels.iter() {
                    if all_tbls.iter()
                        .any(|t| &t.0[..] == &rel.tgt_schema[..] && &t.1[..] == &rel.tgt_tbl[..] )
                    {
                        let tgt = &rel.tgt_tbl;
                        let src_col = format!("{}.{}", name, rel.src_col);
                        let tgt_col = format!("{}.{}", rel.tgt_tbl, rel.tgt_col);
                        tbl += &format!("{name} -- {tgt} [label=\"{src_col}\n{tgt_col}\"];\n");
                    }
                }

                er += &tbl[..];
            },
            _ => { }
        }
    }
    /*if disconnected.len() >= 2 {
        for i in 1..disconnected.len() {
            let j = i-1;
            // for j in ((i+1)..disconnected.len()) {
            let a = &disconnected[i];
            let b = &disconnected[j];
            er += &format!("{a} -- {b} [style=invis];\n");
        }
    }
    if disconnected.len() >= 3 {
        let a = &disconnected[0];
        let b = &disconnected[disconnected.len()-1];
        er += &format!("{a} -- {b} [style=invis];\n");
    }*/
    er
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

impl DBType {

    pub fn requires_quotes(&self) -> bool {
        match self {
            Self::Bool | Self::I16 | Self::I32 | Self::I64 | Self::F32 | Self::F64 | Self::Numeric => false,
            _ => true
        }
    }

    pub fn name(&self) -> &'static str {
        match self {
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
        }
    }
}

impl FromStr for DBType {

    type Err = ();

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        // The underline prefix to array types is set by the return of proargtypes join typename,
        // the bracket postfix is the return of pg_get_function_identity_arguments.
        if s.starts_with('_') || s.ends_with("[]") {
            return Ok(Self::Array)
        }

        // The uppercase versions are usually returned by sqlite's pragma.
        match s {
            "boolean" | "bool" | "BOOL" => Ok(Self::Bool),
            "bigint" | "bigserial" | "int8" => Ok(Self::I64),
            "bit" | "bit varying" | "character" | "character varying" | "text" | "name" | "char" | "cstring" |
                "CHAR" | "CHARACTER" | "TEXT" =>
            {
                Ok(Self::Text)
            },

            "date" | "DATE" => Ok(Self::Date),
            "json" | "jsonb" | "record" => Ok(Self::Json),
            "numeric" => Ok(Self::Numeric),
            "integer" | "int" | "INTEGER" | "INT" | "int4" => Ok(Self::I32),
            "smallint" | "smallserial" | "int2" => Ok(Self::I16),
            "real" | "REAL" | "float4" => Ok(Self::F32),
            "dp" | "double precision" | "float8" => Ok(Self::F64),
            "blob" | "BLOB" | "bytea" | "BYTEA" => Ok(Self::Bytes),
            "time" | "time with time zone" | "time without time zone" |
            "timestamp with time zone" | "timestamp without time zone" => Ok(Self::Time),
            "xml" => Ok(Self::Xml),
            "anyarray" | "array" | "ARRAY" => Ok(Self::Array),
            "trigger" => Ok(Self::Trigger),
            _ => Ok(Self::Unknown)
        }
    }

}

impl fmt::Display for DBType {

    fn fmt(&self, f : &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.name())
    }

}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Relation {
    pub tgt_schema : String,
    pub tgt_tbl : String,
    pub src_col : String,
    pub tgt_col : String
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DBColumn {
    pub name : String,
    pub ty : DBType,
    pub is_pk : bool
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum DBObject {

    // In practice, children will always hold table variants.
    Schema{ name : String, children : Vec<DBObject> },

    Table{ schema : String, name : String, cols : Vec<DBColumn>, rels : Vec<Relation> },

    Function { schema : String, name : String, args : Vec<DBType>, arg_names : Option<Vec<String>>, ret : Option<DBType> },

    View { schema : String, name : String, cols : Vec<DBColumn> }

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
                if name == crate::server::PG_PUB || name.is_empty() {
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

impl fmt::Display for DBObject {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let name : &str = match &self {
            DBObject::Schema{ name, .. } => name,
            DBObject::Table{ name, ..} => name,
            DBObject::Function{ name, ..} => name,
            DBObject::View{ name, ..} => name
        };
        write!(f, "{}", name)
    }
}

