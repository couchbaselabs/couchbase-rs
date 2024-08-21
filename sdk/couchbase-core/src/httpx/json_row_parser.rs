use crate::httpx::error::Result as HttpxResult;
use serde::de::DeserializeOwned;
use std::cmp::PartialEq;
use std::io::Read;

#[derive(Debug)]
pub(crate) struct JsonRowParser {
    buffer: Vec<u8>,
    level: u32,
    curly_braces: u32,
    square_braces: u32,
    in_string: bool,
    send_end_arr: bool,
    sent_end_arr: bool,
    send_end_object: bool,
    sent_end_object: bool,
    last_was_escape: bool,
    i: usize,
}

impl JsonRowParser {
    pub(crate) fn new(level: u32) -> Self {
        JsonRowParser {
            buffer: Vec::new(),
            level,
            curly_braces: 0,
            square_braces: 0,
            in_string: false,
            send_end_arr: false,
            sent_end_arr: false,
            send_end_object: false,
            sent_end_object: false,
            last_was_escape: false,
            i: 0,
        }
    }

    fn within_level(&mut self) -> bool {
        self.curly_braces < self.level
    }

    pub(crate) fn push(&mut self, bytes: &[u8]) {
        self.buffer.extend(bytes);
    }

    fn next_value(&mut self, inclusive: bool) -> Vec<u8> {
        let i = if inclusive { self.i } else { self.i - 1 };
        let res = self.buffer[0..i].to_vec();
        self.drain();
        self.i = 0;
        res
    }

    fn drain(&mut self) {
        self.buffer.drain(0..self.i); // Remove processed elements from the buffer
    }

    pub(crate) fn next(&mut self) -> HttpxResult<Option<Vec<u8>>> {
        loop {
            // Edge case with final }
            if self.send_end_object && !self.sent_end_object {
                self.send_end_object = false;
                self.sent_end_object = true;
                return Ok(Some(vec![b'}']));
            }

            // Edge case with end of rows ]
            if self.send_end_arr && !self.sent_end_arr {
                self.send_end_arr = false;
                self.sent_end_arr = true;
                self.drain();
                return Ok(Some(vec![b']']));
            }

            let buff_length = self.buffer.len();
            if self.i == buff_length {
                return Ok(None);
            }

            let next_char = self.buffer[self.i] as char;
            self.i += 1;

            if self.in_string {
                if self.last_was_escape {
                    self.last_was_escape = false;
                } else if next_char == '"' {
                    self.in_string = false;
                } else if next_char == '\\' {
                    self.last_was_escape = true;
                }
            } else {
                match next_char {
                    '[' => {
                        if self.within_level() {
                            self.square_braces += 1;
                            return Ok(Some(self.next_value(true)));
                        }
                    }
                    '{' => {
                        if self.curly_braces == 0 && self.square_braces == 0 {
                            self.curly_braces += 1;
                            return Ok(Some(self.next_value(true)));
                        } else {
                            self.curly_braces += 1;
                        }
                    }
                    ',' => {
                        if self.within_level() {
                            // Edge case to account for , at the end of the rows
                            if self.sent_end_arr {
                                self.sent_end_arr = false;
                                self.buffer.remove(self.i - 1);
                                self.i -= 1;
                                continue;
                            }
                            return Ok(Some(self.next_value(false)));
                        }
                    }
                    '"' => {
                        self.in_string = true;
                    }
                    ']' => {
                        if self.within_level() {
                            self.square_braces -= 1;
                            self.send_end_arr = true;
                            // Edge case to account for empty rows. i > 1 means rows were non-empty, so send final row, else drain and reset cursor
                            if self.i > 1 {
                                return Ok(Some(self.next_value(false)));
                            } else {
                                self.drain();
                                self.i = 0;
                            }
                        }
                    }
                    '}' => {
                        self.curly_braces -= 1;
                        if self.curly_braces == 0 {
                            self.send_end_object = true;
                            return Ok(Some(self.next_value(false)));
                        }
                    }
                    other => {
                        if other.is_whitespace() {
                            // Ignore any whitespace not within a string
                            self.buffer.remove(self.i - 1);
                            self.i -= 1;
                        }
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::JsonRowParser;

    #[test]
    fn successful_query_response() {
        let json = b"{\n\"requestID\": \"5be66457-d623-45e9-a4ae-9da888ee53bb\",\n\"signature\": {\"*\":\"*\"},\n\"results\": [\n{\"travel-sample\":{\"id\":10,\"type\":\"airline\",\"name\":\"40-Mile Air\",\"iata\":\"Q5\",\"icao\":\"MLA\",\"callsign\":\"MILE-AIR\",\"country\":\"United States\"}},\n{\"travel-sample\":{\"id\":10123,\"type\":\"airline\",\"name\":\"Texas Wings\",\"iata\":\"TQ\",\"icao\":\"TXW\",\"callsign\":\"TXW\",\"country\":\"United States\"}}\n],\n\"status\": \"success\",\n\"metrics\": {\"elapsedTime\": \"1.748019ms\",\"executionTime\": \"1.680107ms\",\"resultCount\": 2,\"resultSize\": 274,\"serviceLoad\": 6}\n}\n";
        let mut parser = JsonRowParser::new(2);
        let mut res = Vec::new();

        parser.push(json);
        while let Some(next) = parser.next().unwrap() {
            res.push(String::from_utf8(next).unwrap());
        }

        assert_eq!(
            res,
            vec![
                r#"{"#,
                r#""requestID":"5be66457-d623-45e9-a4ae-9da888ee53bb""#,
                r#""signature":{"*":"*"}"#,
                r#""results":["#,
                r#"{"travel-sample":{"id":10,"type":"airline","name":"40-Mile Air","iata":"Q5","icao":"MLA","callsign":"MILE-AIR","country":"United States"}}"#,
                r#"{"travel-sample":{"id":10123,"type":"airline","name":"Texas Wings","iata":"TQ","icao":"TXW","callsign":"TXW","country":"United States"}}"#,
                r#"]"#,
                r#""status":"success""#,
                r#""metrics":{"elapsedTime":"1.748019ms","executionTime":"1.680107ms","resultCount":2,"resultSize":274,"serviceLoad":6}"#,
                r#"}"#,
            ]
        )
    }

    #[test]
    fn pre_rows_query_response() {
        let json = b"{\n\"requestID\": \"848c8bc3-6b76-4a22-9a59-8cbaf2b7d5b9\",\n\"errors\": [{\"code\":1050,\"msg\":\"No statement or prepared value\"}],\n\"status\": \"fatal\",\n\"metrics\": {\"elapsedTime\": \"110.826\xc2\xb5s\",\"executionTime\": \"37.414\xc2\xb5s\",\"resultCount\": 0,\"resultSize\": 0,\"serviceLoad\": 0,\"errorCount\": 1}\n}\n";

        let mut parser = JsonRowParser::new(2);
        let mut res = Vec::new();

        parser.push(json);
        while let Some(next) = parser.next().unwrap() {
            res.push(String::from_utf8(next).unwrap());
        }

        assert_eq!(
            res,
            vec![
                r#"{"#,
                r#""requestID":"848c8bc3-6b76-4a22-9a59-8cbaf2b7d5b9""#,
                r#""errors":["#,
                r#"{"code":1050,"msg":"No statement or prepared value"}"#,
                r#"]"#,
                r#""status":"fatal""#,
                r#""metrics":{"elapsedTime":"110.826µs","executionTime":"37.414µs","resultCount":0,"resultSize":0,"serviceLoad":0,"errorCount":1}"#,
                r#"}"#,
            ]
        )
    }

    #[test]
    fn empty_rows_query_response() {
        let json = b"{\n\"requestID\": \"e245a21e-9a63-4095-bf13-f44967adc251\",\n\"signature\": {\"*\":\"*\"},\n\"results\": [\n],\n\"status\": \"success\",\n\"metrics\": {\"elapsedTime\": \"10.815611ms\",\"executionTime\": \"10.75665ms\",\"resultCount\": 0,\"resultSize\": 0,\"serviceLoad\": 6}\n}\n";
        let mut parser = JsonRowParser::new(2);
        let mut res = Vec::new();

        parser.push(json);
        while let Some(next) = parser.next().unwrap() {
            res.push(String::from_utf8(next).unwrap());
        }

        assert_eq!(
            res,
            vec![
                r#"{"#,
                r#""requestID":"e245a21e-9a63-4095-bf13-f44967adc251""#,
                r#""signature":{"*":"*"}"#,
                r#""results":["#,
                r#"]"#,
                r#""status":"success""#,
                r#""metrics":{"elapsedTime":"10.815611ms","executionTime":"10.75665ms","resultCount":0,"resultSize":0,"serviceLoad":6}"#,
                r#"}"#,
            ]
        )
    }

    #[test]
    fn mid_row_error_query_response() {
        let json = b"{\n\"requestID\": \"0f5f1162-eddc-4fde-b9d5-c43856bd9345\",\n\"signature\": {\"*\":\"*\"},\n\"results\": [\n{\"travel-sample\":{\"id\":10,\"type\":\"airline\",\"name\":\"40-Mile Air\",\"iata\":\"Q5\",\"icao\":\"MLA\",\"callsign\":\"MILE-AIR\",\"country\":\"United States\"}}\n],\n\"errors\": [{\"code\":1080,\"msg\":\"Timeout 5ms exceeded\",\"retry\":true}],\n\"status\": \"fatal\",\n\"metrics\": {\"elapsedTime\": \"5.430038ms\",\"executionTime\": \"5.347622ms\",\"resultCount\": 21,\"resultSize\": 2934,\"serviceLoad\": 6,\"errorCount\": 1}\n}\n";
        let mut parser = JsonRowParser::new(2);
        let mut res = Vec::new();

        parser.push(json);
        while let Some(next) = parser.next().unwrap() {
            res.push(String::from_utf8(next).unwrap());
        }

        assert_eq!(
            res,
            vec![
                r#"{"#,
                r#""requestID":"0f5f1162-eddc-4fde-b9d5-c43856bd9345""#,
                r#""signature":{"*":"*"}"#,
                r#""results":["#,
                r#"{"travel-sample":{"id":10,"type":"airline","name":"40-Mile Air","iata":"Q5","icao":"MLA","callsign":"MILE-AIR","country":"United States"}}"#,
                r#"]"#,
                r#""errors":["#,
                r#"{"code":1080,"msg":"Timeout 5ms exceeded","retry":true}"#,
                r#"]"#,
                r#""status":"fatal""#,
                r#""metrics":{"elapsedTime":"5.430038ms","executionTime":"5.347622ms","resultCount":21,"resultSize":2934,"serviceLoad":6,"errorCount":1}"#,
                r#"}"#,
            ]
        )
    }

    #[test]
    fn search_response() {
        let json = b"{\"status\":{\"total\":1,\"failed\":0,\"successful\":1,\"errors\":{}},\"request\":{\"query\":{\"match_phrase\":\"hop beer\",\"fuzziness\":0},\"size\":100,\"from\":0,\"highlight\":null,\"fields\":null,\"facets\":null,\"explain\":false,\"sort\":[\"-_score\"],\"includeLocations\":false,\"search_after\":null,\"search_before\":null,\"knn\":null,\"knn_operator\":\"\"},\"hits\":[{\"index\":\"beer-search_5cfa71e532c6d666_4c1c5584\",\"id\":\"deschutes_brewery-hop_henge_imperial_ipa\",\"score\":0.4254951021817746,\"locations\":{\"description\":{\"beer\":[{\"pos\":96,\"start\":583,\"end\":587,\"array_positions\":null}],\"hop\":[{\"pos\":95,\"start\":579,\"end\":582,\"array_positions\":null}]}},\"sort\":[\"_score\"]},{\"index\":\"beer-search_5cfa71e532c6d666_4c1c5584\",\"id\":\"harpoon_brewery_boston-glacier_harvest_09_wet_hop_100_barrel_series_28\",\"score\":0.3927991190283264,\"locations\":{\"description\":{\"beer\":[{\"pos\":22,\"start\":130,\"end\":134,\"array_positions\":null}],\"hop\":[{\"pos\":21,\"start\":126,\"end\":129,\"array_positions\":null}]}},\"sort\":[\"_score\"]}],\"total_hits\":2,\"cost\":22201,\"max_score\":0.4254951021817746,\"took\":240338,\"facets\":null}\n";
        let mut parser = JsonRowParser::new(2);
        let mut res = Vec::new();

        parser.push(json);
        while let Some(next) = parser.next().unwrap() {
            res.push(String::from_utf8(next).unwrap());
        }

        assert_eq!(
            res,
            vec![
                r#"{"#,
                r#""status":{"total":1,"failed":0,"successful":1,"errors":{}}"#,
                r#""request":{"query":{"match_phrase":"hop beer","fuzziness":0},"size":100,"from":0,"highlight":null,"fields":null,"facets":null,"explain":false,"sort":["-_score"],"includeLocations":false,"search_after":null,"search_before":null,"knn":null,"knn_operator":""}"#,
                r#""hits":["#,
                r#"{"index":"beer-search_5cfa71e532c6d666_4c1c5584","id":"deschutes_brewery-hop_henge_imperial_ipa","score":0.4254951021817746,"locations":{"description":{"beer":[{"pos":96,"start":583,"end":587,"array_positions":null}],"hop":[{"pos":95,"start":579,"end":582,"array_positions":null}]}},"sort":["_score"]}"#,
                r#"{"index":"beer-search_5cfa71e532c6d666_4c1c5584","id":"harpoon_brewery_boston-glacier_harvest_09_wet_hop_100_barrel_series_28","score":0.3927991190283264,"locations":{"description":{"beer":[{"pos":22,"start":130,"end":134,"array_positions":null}],"hop":[{"pos":21,"start":126,"end":129,"array_positions":null}]}},"sort":["_score"]}"#,
                r#"]"#,
                r#""total_hits":2"#,
                r#""cost":22201"#,
                r#""max_score":0.4254951021817746"#,
                r#""took":240338"#,
                r#""facets":null"#,
                r#"}"#,
            ]
        )
    }

    #[test]
    fn errored_search_response() {
        let json = b"{\"error\":\"rest_auth: preparePerms, err: index not found\",\"request\":{\"query\":{\"match_phrase\":\"hop beer\"},\"size\":100},\"status\":\"fail\"}\n";

        let mut parser = JsonRowParser::new(2);
        let mut res = Vec::new();

        parser.push(json);
        while let Some(next) = parser.next().unwrap() {
            res.push(String::from_utf8(next).unwrap());
        }

        assert_eq!(
            res,
            vec![
                r#"{"#,
                r#""error":"rest_auth: preparePerms, err: index not found""#,
                r#""request":{"query":{"match_phrase":"hop beer"},"size":100}"#,
                r#""status":"fail""#,
                r#"}"#,
            ]
        )
    }
}
