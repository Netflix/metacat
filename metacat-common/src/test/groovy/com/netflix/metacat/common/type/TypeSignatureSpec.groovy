/*
 * Copyright 2016 Netflix, Inc.
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *        http://www.apache.org/licenses/LICENSE-2.0
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */

package com.netflix.metacat.common.type

import spock.lang.Ignore
import spock.lang.Specification

import java.util.regex.Matcher
import java.util.regex.Pattern

class TypeSignatureSpec extends Specification {

    def "parse type signatures"(String typeString) {
        expect:
        def prestoTypeString = toPrestoTypeString(typeString)

        where:
        typeString << [
                "{t:(name:chararray)}",
                "map[]",
                "bytearray",
                "long",
                "int",
                "chararray",
                "{(start_date:int, end_date:int)}",
                "Map[chararray]",
                "{t: (client_request_id: chararray,event_utc_ms: long)}",
                "{t: (play_timestamp: long,play_duration: int,play_title_id: int, play_request_id:chararray)}",
                "{t: (credit_request_id: chararray)}",
                "{t: (credit_timestamp: long)}",
                "{(account_id: int,signup_date: int,cancel_date: int,cancel_reason: chararray,voluntary_cancel: int,billing_nbr: int,device: chararray,isp: chararray,sessions: long,playsessions: long,totalhours: double,playhours: double,genre: chararray,network: chararray,errorrate: long,startplayabortrate: long,rebufferrate: double,stallrate: double,changerate: double,downswitchrate: double,average_video_rate: int,startplay_video_rate: int,initial235share: long,all_starts_video_rate: int,playdelay: int,rebufferplaydelay: int,seekplaydelay: int,earlyrebufferrate: double,laterebufferrate: double,timetoquality: int,subsequentvideorate: int,initial30videorate: int,playdelay10: int,playdelay30: int,playdelay50: int,playdelay70: int,playdelay90: int)}",
                "string",
                "bag",
                "{(title_id: int)}",
                "double",
                "boolean",
                "float",
                "Integer",
                "{(show_title_id: int,source_title_id: int,title_evidence: chararray,title_evidence_type: chararray,row_context: chararray,presentation_rank_number: int,play_count: int,play_duration: int,vhs_bag: {t: (credit_request_id: {t: (credit_request_id: chararray)},credit_timestamp: {t: (credit_timestamp: long)},titles: {t: (play_timestamp: long,play_duration: int,play_title_id: int,play_request_id: chararray,runtime_minutes: float , play_rank:int)})}, join_type:chararray)}",
                "{(show_title_id: int,source_title_id: int,title_evidence: chararray,title_evidence_type: chararray,row_context: chararray,presentation_rank_number: int,play_count: int,play_duration: int,vhs_bag: {t: (client_request_id: {t: (client_request_id: chararray)},credit_timestamp: {t: (credit_timestamp: long)},titles: {t: (play_timestamp: long,play_duration: int,play_title_id: int,play_request_id: chararray,runtime_minutes: float, play_rank:int)})}, join_type:chararray)}",
                "{(show_title_id:int, presentation_rank_number:int, join_type:chararray, play_duration_mins:double, play_count:int, play_start_time:int, runtime_minutes:double, is_tv:int, pvr_rank:int, is_novel:int, is_title_played:int)}",
                "map",
                "{(show_title_id: int,source_title_id: int,title_evidence: chararray,title_evidence_type: chararray,row_context: chararray,presentation_rank_number: int,play_count: int,play_duration: int,vhs_bag: {t: (credit_request_id: {t: (credit_request_id: chararray)},credit_timestamp: {t: (credit_timestamp: long)},titles: {t: (play_timestamp: long,play_duration: int,play_title_id: int,play_request_id: chararray,runtime_minutes: float , play_rank:int)})}, join_type:chararray,interleaving:chararray)}",
                "{(show_title_id: int,location_id: int,vhs_bag: {t: (view_utc_sec: long,view_duration: int,title_id: int, play_request_id: chararray, runtime_minutes:float, play_rank:int)})}",
                "map[{t:(client_request_id: chararray,event_utc_ms: long)}]",
                "{rows: (track_id: int, sub_root_uuid: chararray, list_type: chararray, item_type: chararray, hasevidence: chararray, listContext: chararray, genre_id: int, evidence: map[], presentation_row_number: int, mmid: {t: (show_title_id: int, source_title_id: int, evidence: chararray, evidenceType: chararray, context: chararray, interleaving: chararray)}, diversity_score: int,random_group: chararray,is_fallback: chararray,expected_reward_score: chararray, track_ids: {track_id: (track_id: int)})}",
                "{(show_title_id: int,location_id: int,vhs_bag: {t: (view_utc_sec: long,view_duration: int,title_id: int, play_request_id: chararray, runtime_minutes:float, play_row: int)})}",
                "{(show_title_id: int,source_title_id: int,title_evidence: chararray,title_evidence_type: chararray,row_context: chararray,presentation_rank_number: int,play_count: int,play_duration: int,vhs_bag: {t: (credit_request_id: {t: (credit_request_id: chararray)},credit_timestamp: {t: (credit_timestamp: long)},titles: {t: (play_timestamp: long,play_duration: int,play_title_id: int,play_request_id: chararray,runtime_minutes: float , play_rank:int)})}, join_type:chararray, interleaving: chararray)}",
                "{(presentation_rank_number: int, show_title_id: int)}"]
    }

    @Ignore
    def toPrestoTypeString(String typeString){
        def tokens = ['}':'>','[':'<',']':'>','#':',',':':' ','{':'array<','(':'row\\(',' ':''];
        def keys = ['}',Pattern.quote('['),']','#',':',' ',Pattern.quote('{')+'[a-zA-Z_0-9_\\s]*[:]?',Pattern.quote('(')]
        //def tokenKeys = [Pattern.quote('{'),'t:','}','\\[',']',Pattern.quote('('),'#',':']
        // Create pattern of the format "%(cat|beverage)%"
        String patternString = "(" + keys.join("|") + ")";
        Pattern pattern = Pattern.compile(patternString);
        Matcher matcher = pattern.matcher(typeString);

        StringBuffer sb = new StringBuffer();
        while(matcher.find()) {
            def match = matcher.group(1)
            def replace = tokens.get(match)
            if( !replace && match.startsWith('{')){
                replace = tokens.get('{')
            }
            matcher.appendReplacement(sb, replace);
        }
        matcher.appendTail(sb);
        return sb.toString()
    }
}
