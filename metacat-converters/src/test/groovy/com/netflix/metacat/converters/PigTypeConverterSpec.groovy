package com.netflix.metacat.converters

import com.facebook.presto.spi.type.TypeManager
import com.facebook.presto.type.TypeRegistry
import com.netflix.metacat.converters.impl.PigTypeConverter
import spock.lang.Shared
import spock.lang.Specification

class PigTypeConverterSpec extends Specification {
    @Shared
    PigTypeConverter converter = new PigTypeConverter()
    @Shared
    TypeManager typeManager = new TypeRegistry()

    def "parse type signatures"(String typeString) {
        expect:
        def prestoType = converter.toType(typeString, typeManager)
        def pigType = converter.fromType(prestoType)
        prestoType == converter.toType(pigType, typeManager)
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
                //"string",
                //"bag",
                "{(title_id: int)}",
                "double",
                //"boolean",
                "float",
                //"Integer",
                "{(show_title_id: int,source_title_id: int,title_evidence: chararray,title_evidence_type: chararray,row_context: chararray,presentation_rank_number: int,play_count: int,play_duration: int,vhs_bag: {t: (credit_request_id: {t: (credit_request_id: chararray)},credit_timestamp: {t: (credit_timestamp: long)},titles: {t: (play_timestamp: long,play_duration: int,play_title_id: int,play_request_id: chararray,runtime_minutes: float , play_rank:int)})}, join_type:chararray)}",
                "{(show_title_id: int,source_title_id: int,title_evidence: chararray,title_evidence_type: chararray,row_context: chararray,presentation_rank_number: int,play_count: int,play_duration: int,vhs_bag: {t: (client_request_id: {t: (client_request_id: chararray)},credit_timestamp: {t: (credit_timestamp: long)},titles: {t: (play_timestamp: long,play_duration: int,play_title_id: int,play_request_id: chararray,runtime_minutes: float, play_rank:int)})}, join_type:chararray)}",
                "{(show_title_id:int, presentation_rank_number:int, join_type:chararray, play_duration_mins:double, play_count:int, play_start_time:int, runtime_minutes:double, is_tv:int, pvr_rank:int, is_novel:int, is_title_played:int)}",
                //"map",
                "{(show_title_id: int,source_title_id: int,title_evidence: chararray,title_evidence_type: chararray,row_context: chararray,presentation_rank_number: int,play_count: int,play_duration: int,vhs_bag: {t: (credit_request_id: {t: (credit_request_id: chararray)},credit_timestamp: {t: (credit_timestamp: long)},titles: {t: (play_timestamp: long,play_duration: int,play_title_id: int,play_request_id: chararray,runtime_minutes: float , play_rank:int)})}, join_type:chararray,interleaving:chararray)}",
                "{(show_title_id: int,location_id: int,vhs_bag: {t: (view_utc_sec: long,view_duration: int,title_id: int, play_request_id: chararray, runtime_minutes:float, play_rank:int)})}",
                "map[{t:(client_request_id: chararray,event_utc_ms: long)}]",
                "{rows: (track_id: int, sub_root_uuid: chararray, list_type: chararray, item_type: chararray, hasevidence: chararray, listContext: chararray, genre_id: int, evidence: map[], presentation_row_number: int, mmid: {t: (show_title_id: int, source_title_id: int, evidence: chararray, evidenceType: chararray, context: chararray, interleaving: chararray)}, diversity_score: int,random_group: chararray,is_fallback: chararray,expected_reward_score: chararray, track_ids: {track_id: (track_id: int)})}",
                "{(show_title_id: int,location_id: int,vhs_bag: {t: (view_utc_sec: long,view_duration: int,title_id: int, play_request_id: chararray, runtime_minutes:float, play_row: int)})}",
                "{(show_title_id: int,source_title_id: int,title_evidence: chararray,title_evidence_type: chararray,row_context: chararray,presentation_rank_number: int,play_count: int,play_duration: int,vhs_bag: {t: (credit_request_id: {t: (credit_request_id: chararray)},credit_timestamp: {t: (credit_timestamp: long)},titles: {t: (play_timestamp: long,play_duration: int,play_title_id: int,play_request_id: chararray,runtime_minutes: float , play_rank:int)})}, join_type:chararray, interleaving: chararray)}",
                "{(presentation_rank_number: int, show_title_id: int)}"]
    }
}
