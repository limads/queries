use postgres::Client;
use serde_json;
use std::str::FromStr;
use postgres::fallible_iterator::FallibleIterator;

pub fn listen_at_channel(conn : &mut Client, ch : &mut (String, String, bool)) {
    if !ch.2 {
        if let Ok(_) = conn.execute(&format!("listen {};", &ch.0)[..], &[]) {
            let mut notifs = conn.notifications();
            let mut notifs_iter = notifs.iter();
            while let Ok(Some(notif)) = notifs_iter.next() {
                let filter = if ch.1.is_empty() {
                    true
                } else {
                    if let Ok(filt) = serde_json::Value::from_str(&ch.1) {
                        if notif.payload().is_empty() {
                            println!("Payload empty");
                            return;
                        }
                        if let Ok(pay) = serde_json::Value::from_str(&notif.payload()) {
                            match (filt, pay) {
                                (serde_json::Value::Object(filt_map), serde_json::Value::Object(pay_map)) => {
                                    let mut match_all = false;
                                    for key_s in filt_map.keys() {
                                        match pay_map.get(&key_s[..]) {
                                            Some(serde_json::Value::String(pay_s)) => {
                                                match &filt_map[key_s] {
                                                    serde_json::Value::String(filt_val) => {
                                                        if &filt_val[..] == &pay_s[..] {
                                                            match_all = true;
                                                        } else {
                                                            match_all = false;
                                                        }
                                                    },
                                                    _ => {
                                                        println!("Filter value expected to be string");
                                                        return;
                                                    }
                                                }
                                            },
                                            None => {
                                                match_all = false;
                                            },
                                            _ => {
                                                println!("Payload key is not string");
                                                return;
                                            }
                                        }
                                    }
                                    match_all
                                },
                                (serde_json::Value::Object(filt_map), _) => {
                                    println!("Payload is not valid JSON");
                                    return;
                                },
                                _ => {
                                    println!("Filter is not valid JSON");
                                    return;
                                }
                            }
                        } else {
                            println!("Unable to parse payload as JSON");
                            return;
                        }
                    } else {
                        println!("Unable to parse output as JSON");
                        return;
                    }
                };
                if notif.channel() == &ch.0[..] && filter {
                    // Queue notification to be read by GUI thread.
                    ch.2 = true;
                }
            }
        }
    }
}
