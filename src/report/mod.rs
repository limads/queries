use quick_xml::Reader;
use quick_xml::Writer;
use quick_xml::events::{Event, BytesEnd, BytesStart, BytesText, attributes::Attribute };
use std::error::Error;
use std::io::Cursor;
use crate::tables::{field::Field, table::Table};
use std::convert::TryFrom;
use ::plots::Panel;
use std::cmp::{Eq, PartialEq};
use std::fmt;
use either::Either;

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

fn substitute_field(
    writer : &mut Writer<Cursor<Vec<u8>>>,
    table : &Table,
    colname : &str,
    is_html : bool,
    row_ix : usize
) {
    // Search for this txt in the data table columns
	// println!("Looking for {}", txt.trim());
    let col = table.get_column_by_name(colname).unwrap();
    let field = col.at(row_ix).unwrap();

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
                Err(_) => {
                    match Table::try_from(json) {
                        Ok(inner_tbl) => {
                            if is_html {
                                inner_tbl.to_html()
                            } else {
                                inner_tbl.to_ooxml(None, None)
                            }
                        },
                        Err(_) => {
                            println!("Could not parse table from JSON");
                            String::new()
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

	// println!("{}", txt);
	// writer.write_event(Event::End(BytesEnd::borrowed(b"text:p")));
}

pub mod html {

    use super::*;

    // TODO receive row index to produce many reports instead of indexing row 0.
    pub fn substitute_html(table : &Table, txt : &str) -> Result<String, Box<dyn Error>> {
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
                            row_ix
                        );
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
        (b"body", true) | (b"office:body", false) => {
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
pub struct ParseError { }

impl fmt::Display for ParseError {
    fn fmt(&self, f : &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "ParseError: Body of document was not closed")
    }
}

impl Error for ParseError { }

/// Extracts a prelude, body (section that should be repeated for as many rows as the user informed)
/// and postlude.
pub fn extract_body(txt : &str, is_html : bool) -> Result<Document, Box<dyn Error>> {
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
                        }
                        prelude_writer.write_event(event)?;
	                },
	                Section::Body => {
                        body_writer.write_event(event)?;
	                },
	                Section::Postlude => {
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
                            postlude_writer.write_event(event)?;
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
	            if section == Section::Postlude {
                    let prelude = String::from_utf8(prelude_writer.into_inner().into_inner()).unwrap();
                    let postlude = String::from_utf8(postlude_writer.into_inner().into_inner()).unwrap();
                    let body = String::from_utf8(body_writer.into_inner().into_inner()).unwrap();
                    return Ok(Document { body, prelude, postlude });
                } else {
                    return Err(Box::new(ParseError{}));
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
                        row_ix
                    );
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
    row_ix : usize
) {
    if inside_placeholder {
        let colname = get_colname(event_name, reader);
        substitute_field(writer, &table, &colname, is_html, row_ix);
        *copy_current = false;
    } else {
        *copy_current = true;
    }
}

fn process_end(end : &BytesEnd<'_>, end_tag : &[u8], inside_placeholder : &mut bool, copy_current : &mut bool) {
    if end.name() == end_tag {
        *inside_placeholder = false;
        *copy_current = false;
    } else {
        *copy_current = true;
	}
}


