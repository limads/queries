use quick_xml::Reader;
use quick_xml::Writer;
use quick_xml::events::{Event, BytesEnd, BytesStart, BytesText, attributes::Attribute };
use std::error::Error;
use std::io::Cursor;
use crate::tables::{field::Field, table::Table};
use std::convert::TryFrom;
use papyri::render::Panel;
use std::cmp::{Eq, PartialEq};
use std::fmt;
use either::Either;
use std::fs::File;
use std::io::{Read, Write};
use postgres::Client;

// cat data.csv | queries report -l field.fodt > out.fodt

/*
Libreoffice convention:
<table>
<table-column />
<table-column />
<table-row>
    <table-cell>
        <p>
            <span>
                <placeholder>content</placeholder>
            </span>
        </p>
    </table-cell>
</table-row>
*/

#[derive(Debug)]
pub struct SubstitutionError(String);

impl fmt::Display for SubstitutionError {

    fn fmt(&self, f : &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Field '{}' at template does not match any column", self.0)
    }

}

impl std::error::Error for SubstitutionError { }

fn substitute_field(
    writer : &mut Writer<Cursor<Vec<u8>>>,
    table : &Table,
    colname : &str,
    is_html : bool,
    row_ix : usize,
    missing : Option<&str>
) -> Result<(), Box<dyn Error>> {
    // Search for this txt in the data table columns
	// println!("Looking for {}", txt.trim());
    let col = table.get_column_by_name(colname).ok_or(SubstitutionError(colname.to_string()))?;
    let field = col.at(row_ix, missing).unwrap();

    /*let mut start = BytesStart::owned(b"text:p".to_vec(), "text:p".len());
    if let Some(attrs) = curr_p_props.as_ref() {
        start.extend_attributes(attrs.iter().cloned());
    }
    writer.write_event(start);*/

    let content = match field {
        Field::Json(json) => {
            let s = json.to_string();
            match Panel::new_from_json(&s[..]) {
                Ok(mut pl) => {
                    if is_html {

                        // There seems to be a text rendering bug (cairo-rs 0.9; libcairo2 1.16.0)
                        // when a lot of SVG surfaces are rendered in series (some glyphs
                        // within each surface are substituted for each other).
                        // This isn't obseved with the PNG surface.
                        // pl.svg().unwrap()
                        pl.html_img_tag().unwrap()

                    } else {

                        // TODO create hidden dir beside the report file.

                        let path = format!("/home/diego/Downloads/{}.svg", std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos());
                        pl.draw_to_file(&path[..]).unwrap();

                        format!(r#"<draw:frame draw:name="graphics2" svg:width="600px" svg:height="400px">
                          <draw:image xlink:href="{}" xlink:type="simple" xlink:show="embed" xlink:actuate="onLoad" />
                        </draw:frame>"#, path)

                        /*format!(r#"<draw:frame draw:name="graphics1" svg:width="300px" svg:height="300px">
                        <draw:image xlink:href="/home/diego/Downloads/plot.svg" xlink:type="simple" xlink:show="embed" xlink:actuate="onLoad" />
                        </draw:frame>"#)*/

                    }
                },
                Err(e) => {

                    println!("Plot parsing error: {}", e);

                    match Table::try_from(json.clone()) {
                        Ok(inner_tbl) => {
                            if is_html {
                                inner_tbl.to_html()
                            } else {
                                inner_tbl.to_ooxml(None, None)
                            }
                        },
                        Err(e) => {
                            println!("Could not parse table from JSON: {}", e);
                            json.to_string()
                        }
                    }
                }
            }
        },
        other => {
            let content = other.display_content();
            /*if is_html {
                format!("<p>{}</p>", content)
            } else {
                content
            }*/
            content
        }
    };

    // let bytes_txt = BytesText::from_plain(content.as_ref());
    // writer.write_event(Event::Text(bytes_txt));
    writer.write(content.as_bytes());

    Ok(())

	// println!("{}", txt);
	// writer.write_event(Event::End(BytesEnd::borrowed(b"text:p")));
}

pub mod html {

    use super::*;

    // TODO receive row index to produce many reports instead of indexing row 0.
    pub fn substitute_html(table : &Table, txt : &str, missing : Option<&str>) -> Result<String, Box<dyn Error>> {
        // let mut reader = Reader::from_str(txt);
        // let mut writer = Writer::new(Cursor::new(Vec::new()));
	    // reader.trim_text(true);
	    // let mut buf = Vec::new();

        // Perhaps the user can have two options:
        // (1) Repeat the FIELD N times for each row (rendering HTML table), leaving the document as-is
        // (2) Repeat the DOCUMENT N times for each row. In this case, we would need
        // to repeat just the part within <body> (HTML) or <office:body> (OOXML).
        // let row_ix = 0;

        let doc = extract_body(&txt, true)?;
        let mut body_writer = Writer::new(Cursor::new(Vec::new()));
        for row_ix in 0..table.shape().0 {
            let mut reader = Reader::from_str(&doc.body[..]);
            let mut buf = Vec::new();
         	let mut copy_current = false;
            let mut inside_placeholder = false;
            let mut eof = false;

            body_writer.write(b"\n<section>\n");

            loop {
                let event = reader.read_event(&mut buf)?;
                match &event {
	                Event::Start(ref e) => {
	                    match e.name() {
	                        b"template" => {
	                            copy_current = false;
	                            inside_placeholder = true;
	                        },
	                        _ => {
	                            copy_current = true;
	                        }
	                    }
	                },
	                Event::Text(ref ev) => {
	                    substitute_text(
                            &mut body_writer,
                            &mut copy_current,
                            inside_placeholder,
                            &table,
                            ev,
                            &reader,
                            true,
                            row_ix,
                            missing
                        )?;
	                },
	                Event::End(ref e) => {
	                	process_end(e, b"template", &mut inside_placeholder, &mut copy_current);
	                },
	                Event::Eof => {
	                    copy_current = true;
	                    eof = true;
	                }
	                _ => {
	                    copy_current = true;
	                },
	            }

                if copy_current {
	                body_writer.write_event(event)?;
	            }

	            if eof {
	                break;
	            }
            }

            body_writer.write(b"\n</section>\n");
        }

        Ok(render_document(&doc, body_writer))
    }

}

pub struct Document {
    prelude : String,
    body : String,
    postlude : String
}

fn render_document(doc : &Document, body_writer : Writer<Cursor<Vec<u8>>>) -> String {
    let body = String::from_utf8(body_writer.into_inner().into_inner()).unwrap();
    format!("{}\n{}\n{}", doc.prelude, body, doc.postlude)
}

fn is_body_tag(ev : Either<&BytesStart<'_>, &BytesEnd<'_>>, is_html : bool) -> bool {
    let name = match ev {
        Either::Left(ev) => ev.name(),
        Either::Right(ev) => ev.name()
    };
    match (name, is_html) {
        (b"section", true) | (b"office:body", false) => {
            true
        },
        _ => false
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Section {
    Prelude,
    Body,
    Postlude,
}

#[derive(Debug)]
pub struct ParseError(String);

impl fmt::Display for ParseError {
    fn fmt(&self, f : &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl Error for ParseError { }

fn check_not_body(e : &BytesStart<'_>, is_html : bool) -> Result<(), Box<dyn Error>> {
    if is_body_tag(Either::Left(e), is_html) {
        let msg = if is_html {
            "Multiple <section> tags found (expected one)"
        } else {
            "Multiple <office:body> tags found (expected one)"
        };
        Err(Box::new(ParseError(String::from(msg))))
    } else {
        Ok(())
    }
}

/// Extracts a prelude, body (section that should be repeated for as many rows as the user informed)
/// and postlude.
pub fn extract_body(txt : &str, is_html : bool) -> Result<Document, Box<dyn Error>> {

    if txt.is_empty() {
        return Err(Box::new(ParseError(String::from("Empty template document"))));
    }

    let mut reader = Reader::from_str(txt);
    let mut prelude_writer = Writer::new(Cursor::new(Vec::new()));
    let mut body_writer = Writer::new(Cursor::new(Vec::new()));
    let mut postlude_writer = Writer::new(Cursor::new(Vec::new()));
	reader.trim_text(true);
	let mut section = Section::Prelude;
	let mut buf = Vec::new();
    loop {
        let event = reader.read_event(&mut buf)?;
        match &event {
	        Event::Start(ref e) => {
	            match section {
	                Section::Prelude => {
	                    if is_body_tag(Either::Left(e), is_html) {
                            section = Section::Body;
                            // For HTML, the start and end <section> tags are repeated for every row.
                            // For OOXML, we have only one start/end tag.
                            if !is_html {
                                prelude_writer.write_event(event)?;
                            }
                        } else {
                            prelude_writer.write_event(event)?;
                        }
	                },
	                Section::Body => {
	                    check_not_body(e, is_html)?;
                        body_writer.write_event(event)?;
	                },
	                Section::Postlude => {
	                    check_not_body(e, is_html)?;
                        postlude_writer.write_event(event)?;
	                }
	            }
	        },
	        Event::End(ref e) => {
	             match section {
	                Section::Prelude => {
                        prelude_writer.write_event(event)?;
	                },
	                Section::Body => {
	                    if is_body_tag(Either::Right(e), is_html) {
                            section = Section::Postlude;

                            // For HTML, the start and end <section> tags are repeated for every row.
                            // For OOXML, we have only one start/end tag.
                            if !is_html {
                                postlude_writer.write_event(event)?;
                            }

                            // End event not written for HTML, because it will be
                            // inserted at the field substitution stage.

                        } else {
                            body_writer.write_event(event)?;
                        }
	                },
	                Section::Postlude => {
                        postlude_writer.write_event(event)?;
	                }
	            }
	        },
	        Event::Eof => {
	            let prelude = String::from_utf8(prelude_writer.into_inner().into_inner()).unwrap();
                let postlude = String::from_utf8(postlude_writer.into_inner().into_inner()).unwrap();
                let body = String::from_utf8(body_writer.into_inner().into_inner()).unwrap();
	            if section == Section::Postlude {
                    return Ok(Document { body, prelude, postlude });
                } else {
                    println!("prelude = {};", prelude);
                    println!("body = {};", body);
                    println!("postlude = {};", postlude);
                    return Err(Box::new(ParseError(String::from("ParseError: Body of document was not closed"))));
                }
	        },
	        _ => {
                match section {
	                Section::Prelude => {
                        prelude_writer.write_event(event)?;
                    },
                    Section::Body => {
                        body_writer.write_event(event)?;
                    },
                    Section::Postlude => {
                        postlude_writer.write_event(event);
                    }
                }
	        }
	    }
    }
    panic!()
}

pub mod ooxml {

    use super::*;

    // TODO receive row index to produce many reports instead of indexing row 0.
    pub fn substitute_ooxml(table : &Table, txt : &str) -> Result<String, Box<dyn Error>> {
        let mut reader = Reader::from_str(txt);
        let mut writer = Writer::new(Cursor::new(Vec::new()));
	    reader.trim_text(true);
	    let mut buf = Vec::new();
	    let mut curr_p_props : Option<Vec<Attribute>> = None;
        let mut copy_current = false;
        let mut inside_placeholder = false;
        let mut eof = false;

        let row_ix = 0;
	    loop {
	        let event = reader.read_event(&mut buf)?;
		    match &event {
		        Event::Start(ref e) => {
		            match e.name() {
		                b"text:placeholder" => {
		                    copy_current = false;
		                    inside_placeholder = true;
		                },
		                b"text:p" => {
                            curr_p_props = Some(e.attributes().map(|attr| attr.unwrap() ).collect::<Vec<_>>());
                            copy_current = true;
		                },
		                _ => {
		                    copy_current = true;
		                }
		            }
		        },
		        Event::Text(ref ev_name) => {
		            substitute_text(
                        &mut writer,
                        &mut copy_current,
                        inside_placeholder,
                        &table,
                        ev_name,
                        &reader,
                        false,
                        row_ix,
                        None
                    )?;
		        },
		        Event::End(ref e) => {
		            process_end(e, b"text:placeholder", &mut inside_placeholder, &mut copy_current);
		        },
		        Event::Eof => {
		            copy_current = true;
		            eof = true;
		        }
		        _ => {
		            copy_current = true;
		        },
		    }

		    if copy_current {
		        writer.write_event(event)?;
		    }

		    if eof {
		        break;
		    }

		    // When we need to substitute:
		    // elem.extend_attributes();
		    // writer.write_event(Event::Start(elem))
		    // writer.write_event(Event::End(BytesEnd::borrowed(b"my_elem")));
	    }

	    let result = writer.into_inner().into_inner();
        Ok(String::from_utf8(result).unwrap())
    }

}

fn get_colname(event_name : &BytesText<'_>, reader : &Reader<& [u8]>) -> String {
    let txt = event_name.unescape_and_decode(&reader).unwrap();
    let txt = txt.replace("<", "").replace(">", "");
    txt.trim().to_string()
}

fn substitute_text(
    writer : &mut Writer<Cursor<Vec<u8>>>,
    copy_current : &mut bool,
    inside_placeholder : bool,
    table : &Table,
    event_name : &BytesText<'_>,
    reader : &Reader<& [u8]>,
    is_html : bool,
    row_ix : usize,
    missing : Option<&str>
) -> Result<(), Box<dyn Error>> {
    if inside_placeholder {
        let colname = get_colname(event_name, reader);
        substitute_field(writer, &table, &colname, is_html, row_ix, missing)?;
        *copy_current = false;
    } else {
        *copy_current = true;
    }
    Ok(())
}

fn process_end(end : &BytesEnd<'_>, end_tag : &[u8], inside_placeholder : &mut bool, copy_current : &mut bool) {
    if end.name() == end_tag {
        *inside_placeholder = false;
        *copy_current = false;
    } else {
        *copy_current = true;
	}
}

pub fn make_report(
    client : &mut Client,
    sql : &str,
    layout_path : String,
    output_path : Option<String>
) -> Result<(), String> {
    match client.query(sql, &[]) {
        Ok(rows) => {
            let tbl = Table::from_rows(&rows[..]).map_err(|e| format!("{}", e) )?;
            launch_report(&tbl, layout_path, output_path)
        },
        Err(e) => Err(format!("{}", e) )
    }
}

pub fn launch_report(
    tbl : &Table,
    layout_path : String,
    output_path : Option<String>,
) -> Result<(), String> {
    let layout_ext = std::path::Path::new(&layout_path).extension().and_then(|ext| ext.to_str() );
    let mut content = String::new();
    let mut f = File::open(&layout_path).map_err(|e| format!("{}", e) )?;
    f.read_to_string(&mut content).map_err(|e| format!("{}", e) )?;
    let out_data = match layout_ext {
        Some("html") => {
            html::substitute_html(tbl, &content[..], None)
                .map_err(|e| format!("{}", e) )?
        },
        Some("fodt") => {
            ooxml::substitute_ooxml(tbl, &content[..])
                .map_err(|e| format!("{}", e) )?
        },
        _ => {
            Err(format!("Invalid or missing file extension. Should be .html or .fodt"))?
        }
    };
    if let Some(out) = output_path {

        let out_ext = std::path::Path::new(&out).extension().and_then(|ext| ext.to_str() );
        if out_ext != layout_ext {
            Err(format!("Layout and output files should have the same extension"))?
        }

        File::create(out).map_err(|e| format!("{}", e) )?
            .write_all(out_data.as_bytes())
            .map_err(|e| format!("{}", e) )?;
    } else {
        println!("{}", out_data);
    }

    Ok(())
}

