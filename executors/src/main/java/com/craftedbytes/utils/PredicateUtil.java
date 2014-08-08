package com.craftedbytes.utils;

import com.hazelcast.query.EntryObject;
import com.hazelcast.query.Predicate;
import com.hazelcast.query.PredicateBuilder;
import com.hazelcast.query.Predicates;

import java.util.ArrayList;
import java.util.List;

//import a.a.a.a.a.e;

public class PredicateUtil {

	public static final String QUERY2 = "disabled_for_upload = false " + //Done
			" AND ( media_id != -1 AND inactive = false AND quality_check_state = 12 AND proxy_creation_state = 14 " + //Done
			" AND ( ( media_content_approval_instruction != 3 AND content_approval_media_state != 19 AND content_approval_media_state != 17) " + // Done
			" OR ( media_clock_approval_instruction != 31 AND clock_approval_media_state != 41) OR ( media_instruction_approval_instruction != 51 " + //Done
			" AND ( num_instruction_approval_not_performed > 0 OR num_instruction_rejected > 0)) OR ( media_class_approval_instruction != 41 " + //Done
			" AND class_approval_media_state != 71) OR ( media_house_nr = '' AND media_house_nr_instruction = 10)) AND ( content_approval_media_state != 18 " + //Done
			" AND clock_approval_media_state != 42 AND class_approval_media_state != 72 AND num_instruction_rejected = 0 AND qc_approval_media_state != 22 " + //Done
			" AND approval_advert_state != 11) AND ( aired = false) AND ( approval_advert_state = 10 OR approval_advert_state = 9) AND ( NOT ( time_delivered <= 0 " + //Done
			" AND stopped = false AND inactive = false AND quality_check_state = 12 AND proxy_creation_state = 14 AND ( content_approval_media_state = 19 " + //Done
			" OR content_approval_media_state = 17 OR media_content_approval_instruction != 1) AND ( class_approval_media_state = 71 OR media_class_approval_instruction != 40) " + //Done
			"AND ( clock_approval_media_state = 41 OR media_clock_approval_instruction != 30) " + //Done
			"AND ( media_instruction_approval_instruction != 50 OR ( num_instruction_approval_not_performed = 0 AND num_instruction_rejected = 0)) " + //Done
			"AND ( approval_advert_state = 10 OR approval_advert_state = 9) AND ( media_house_nr != '' OR media_house_nr_instruction = 11))) " + //Done
			"AND ( media_id > 60000000))";
	
	public static Predicate mediumComplexQuery(){
		/*Predicate disabledForUpload = Predicates.equal("disabled_for_upload", false);
		Predicate mediaId1 = Predicates.notEqual("media_id", -1);
		Predicate inactive = Predicates.equal("inactive", false);
		Predicate qualityCheckState = Predicates.equal("quality_check_state", 12);
		Predicate proxyCreationState = Predicates.equal("proxy_creation_state", 14);
		Predicate mediaContentApprovalInstruction1 = Predicates.notEqual("media_content_approval_instruction", 3);
		Predicate contentApprovalMediaState1 = Predicates.notEqual("content_approval_media_state", 19);
		Predicate contentApprovalMediaState2 = Predicates.notEqual("content_approval_media_state", 17);
		Predicate mediaClockApprovalInstruction = Predicates.notEqual("media_clock_approval_instruction", 31);
		Predicate clockApprovalMediaState1 = Predicates.notEqual("clock_approval_media_state", 41);
		Predicate mediaInstructionApprovalInstruction1 = Predicates.notEqual("media_instruction_approval_instruction", 51);
		Predicate numInstructionApprovalNotPerformed1 = Predicates.greaterThan("num_instruction_approval_not_performed", 0);
		Predicate numInstructionRejected1 = Predicates.greaterThan("num_instruction_rejected", 0);
		Predicate mediaClassApprovalInstruction1 = Predicates.notEqual("media_class_approval_instruction", 41);
		Predicate classApprovalMediaState1 = Predicates.notEqual("class_approval_media_state", 71);
		Predicate mediaHouseNr1 = Predicates.equal("media_house_nr", "");
		Predicate mediaHouseNrInstruction1 = Predicates.equal("media_house_nr_instruction", 10);
		Predicate contentApprovalMediaState = Predicates.notEqual("content_approval_media_state", 18);
		Predicate clockApprovalMediaState2 = Predicates.notEqual("clock_approval_media_state", 42);
		Predicate classApprovalMediaState2 = Predicates.notEqual("class_approval_media_state", 72);
		Predicate numInstructionRejected2 = Predicates.equal("num_instruction_rejected", 0);
		Predicate qcApprovalMediaState = Predicates.notEqual("qc_approval_media_state", 22);
		Predicate advertApprovalState1 = Predicates.notEqual("approval_advert_state", 11);
		Predicate aired = Predicates.equal("aired", false);
		Predicate advertApprovalState2 = Predicates.equal("approval_advert_state", 10);
		Predicate advertApprovalState3 = Predicates.equal("approval_advert_state", 9);
		Predicate timeDelivered = Predicates.lessEqual("time_delivered", 0);
		Predicate stopped = Predicates.equal("stopped", false);
		Predicate mediaContentApprovalInstruction2 = Predicates.notEqual("media_content_approval_instruction", 1);
		Predicate mediaClassApprovalInstruction2 = Predicates.notEqual("media_class_approval_instruction", 40);
		Predicate clockApprovalMediaState3 = Predicates.equal("clock_approval_media_state", 40);
		Predicate mediaClockApprovalInstructio = Predicates.notEqual("media_clock_approval_instruction", 30);
		Predicate mediaInstructionApprovalInstruction2 = Predicates.notEqual("media_instruction_approval_instruction", 50);
		Predicate numInstructionApprovalNotPerformed2 = Predicates.equal("num_instruction_approval_not_performed", 0);
		Predicate numInstructionRejected3 = Predicates.equal("num_instruction_rejected", 0);
		Predicate mediaHouseNr2 = Predicates.notEqual("media_house_nr", "");
		Predicate mediaHouseNrInstruction2 = Predicates.equal("media_house_nr_instruction", 11);
		Predicate mediaId2 = Predicates.greaterThan("mediaId", 60000000);*/
		
		EntryObject eo = new PredicateBuilder().getEntryObject();
		Predicate p = eo.isNot("disabled_for_upload")
				.and(eo.get("media_id").notEqual(-1))
				.and(eo.isNot("inactive"))
				.and(eo.get("quality_check_state").equal(12))
				.and(eo.get("proxy_creation_state").equal(14))
				.and(eo.get("content_approval_media_state").notEqual(18))
				.and(eo.get("clock_approval_media_state").notEqual(42))
				.and(eo.get("class_approval_media_state").notEqual(72))
				.and(eo.get("num_instruction_rejected").equal(0))
				.and(eo.get("qc_approval_media_state").notEqual(22))
				.and(eo.get("approval_advert_state").notEqual(11))
				.and(eo.isNot("aired"))
				.and(eo.get("approval_advert_state").in(9,10));
		
		EntryObject eo1 = new PredicateBuilder().getEntryObject();
		Predicate p1 = eo1.get("media_content_approval_instruction").notEqual(3).and(eo1.get("content_approval_media_state").notEqual(19)).and(eo1.get("content_approval_media_state").notEqual(17));
		
		EntryObject eo2 = new PredicateBuilder().getEntryObject();
		Predicate p2 = eo2.get("media_clock_approval_instruction").notEqual(31).and(eo2.get("clock_approval_media_state").notEqual(41));
		
		EntryObject eo3 = new PredicateBuilder().getEntryObject();
		Predicate p3 = eo3.get("media_instruction_approval_instruction").notEqual(51)
				.and(eo3.get("num_instruction_approval_not_performed").greaterThan(0)
						.or(eo3.get("num_instruction_rejected").greaterThan(0)
					)
				);
		EntryObject eo4 = new PredicateBuilder().getEntryObject();
		Predicate p4 = eo4.get("media_class_approval_instruction").notEqual(41).and(eo4.get("class_approval_media_state").notEqual(71));
		
		EntryObject eo5 = new PredicateBuilder().getEntryObject();
		Predicate p5 = eo5.get("media_house_nr").equal("").and(eo5.get("media_house_nr_instruction").equal(10));
		
		Predicate pOr1 = Predicates.or(p1, p2);
		pOr1 = Predicates.or(pOr1, p3);
		pOr1 = Predicates.or(pOr1, p4);
		pOr1 = Predicates.or(pOr1, p5);
		
		Predicate pAnd = Predicates.and(p, pOr1);
		
		EntryObject eo6 = new PredicateBuilder().getEntryObject();
		Predicate p6 = eo6.get("time_delivered").lessEqual(0)
				.and(eo6.isNot("stopped"))
				.and(eo6.isNot("inactive"))
				.and(eo6.get("quality_check_state").equal(12))
				.and(eo6.get("proxy_creation_state").equal(14))
				.and(eo6.get("content_approval_media_state").equal(19).or(eo6.get("content_approval_media_state").equal(17).or(eo6.get("media_content_approval_instruction").notEqual(1))))
				.and(eo6.get("class_approval_media_state").equal(71).or(eo6.get("media_class_approval_instruction").notEqual(40)))
				.and(eo6.get("clock_approval_media_state").equal(41).or(eo6.get("media_clock_approval_instruction").notEqual(30)))
				.and(eo6.get("media_instruction_approval_instruction").notEqual(50).or(eo6.get("num_instruction_approval_not_performed").equal(0).and(eo6.get("num_instruction_rejected").equal(0))))
				.and(eo6.get("approval_advert_state").in(9,10))
				.and(eo6.get("media_house_nr").notEqual("").or(eo6.get("media_house_nr_instruction").equal(11)));
		
		pAnd = Predicates.and(pAnd, Predicates.not(p6));
		return pAnd;
	}
	
	public static Predicate complexQueryAsPredicate(){

		EntryObject eo = new PredicateBuilder().getEntryObject();
		Predicate p = eo.isNot("disabled_for_upload")
				.and(eo.get("media_id").notEqual(-1))
				.and(eo.isNot("inactive"))
				.and(eo.get("quality_check_state").equal(12))
				.and(eo.get("proxy_creation_state").equal(14))
				.and(eo.get("content_approval_media_state").notEqual(18))
				.and(eo.get("clock_approval_media_state").notEqual(42))
				.and(eo.get("class_approval_media_state").notEqual(72))
				.and(eo.get("num_instruction_rejected").equal(0))
				.and(eo.get("qc_approval_media_state").notEqual(22))
				.and(eo.get("approval_advert_state").notEqual(11))
				.and(eo.isNot("aired"))
				.and(eo.get("approval_advert_state").in(9,10));
		
		EntryObject eo1 = new PredicateBuilder().getEntryObject();
		Predicate p1 = eo1.get("media_content_approval_instruction").notEqual(3).and(eo1.get("content_approval_media_state").notEqual(19)).and(eo1.get("content_approval_media_state").notEqual(17));
		
		EntryObject eo2 = new PredicateBuilder().getEntryObject();
		Predicate p2 = eo2.get("media_clock_approval_instruction").notEqual(31).and(eo2.get("clock_approval_media_state").notEqual(41));
		
		EntryObject eo3 = new PredicateBuilder().getEntryObject();
		Predicate p3 = eo3.get("media_instruction_approval_instruction").notEqual(51)
				.and(eo3.get("num_instruction_approval_not_performed").greaterThan(0)
						.or(eo3.get("num_instruction_rejected").greaterThan(0)
					)
				);
		EntryObject eo4 = new PredicateBuilder().getEntryObject();
		Predicate p4 = eo4.get("media_class_approval_instruction").notEqual(41).and(eo4.get("class_approval_media_state").notEqual(71));
		
		EntryObject eo5 = new PredicateBuilder().getEntryObject();
		Predicate p5 = eo5.get("media_house_nr").equal("").and(eo5.get("media_house_nr_instruction").equal(10));
		
		Predicate pOr1 = Predicates.or(p1, p2);
		pOr1 = Predicates.or(pOr1, p3);
		pOr1 = Predicates.or(pOr1, p4);
		pOr1 = Predicates.or(pOr1, p5);
		
		Predicate pAnd = Predicates.and(p, pOr1);
		
		EntryObject eo6 = new PredicateBuilder().getEntryObject();
		Predicate p6 = eo6.get("time_delivered").lessEqual(0)
				.and(eo6.isNot("stopped"))
				.and(eo6.isNot("inactive"))
				.and(eo6.get("quality_check_state").equal(12))
				.and(eo6.get("proxy_creation_state").equal(14))
				.and(eo6.get("content_approval_media_state").equal(19).or(eo6.get("content_approval_media_state").equal(17).or(eo6.get("media_content_approval_instruction").notEqual(1))))
				.and(eo6.get("class_approval_media_state").equal(71).or(eo6.get("media_class_approval_instruction").notEqual(40)))
				.and(eo6.get("clock_approval_media_state").equal(41).or(eo6.get("media_clock_approval_instruction").notEqual(30)))
				.and(eo6.get("media_instruction_approval_instruction").notEqual(50).or(eo6.get("num_instruction_approval_not_performed").equal(0).and(eo6.get("num_instruction_rejected").equal(0))))
				.and(eo6.get("approval_advert_state").in(9,10))
				.and(eo6.get("media_house_nr").notEqual("").or(eo6.get("media_house_nr_instruction").equal(11)));
		
		pAnd = Predicates.and(pAnd, Predicates.not(p6));

		int[] mediaIds7 = {73474404,18,19,20,21,59673386,59673555,60386838,60386991,60387038,63063042,71930797,71930974,71931106,78158138,121,122,123,74724915,59791484,60474387,60995088,62718106,63355282,63355295,63803353,64695309,67657712,72587686,62017377,64854099,67102704,67102758,67102823,67102923,67102991,67409968,67425958,69581739,69902352,77041794,77042317,77042419,77042607,77133328,77133445,77133519,140,3264387,3264455,3264523,65982154,65982356,141,58721638,59698851,59698923,59805097,59805166,59805235,59805304,66225057,71930557,71930578,55097116,55101161,55103652,55106387,62046263,154,59738411,59738458,59738505,64397729,64897053,66485631,68213411,23,24,25,26,27,60505044,63293532,68582738,71916462,63294136,72,73,74,75,76,77,78,79,80,55874320,59809236,61011675,61886419,64693948,67322242,68273046,70072636};
		List<Integer> l7 = new ArrayList<Integer>();
		for(Integer mediaId : mediaIds7){
			l7.add(mediaId);
		}
		
		int[] mediaIds8 = {74534152,76342761,4294273,67387111,60362913,68253459,60362914,67820,64658131,68384425,71398673,62542181,3263119,59786652,59786673,16128,55831870,58381550,59949741,59820961,60422086,60422095,60422104,65113499,68280362,61162521,62827656,63150848,63152006,61190448,62017376,64854196,65709715,68039591,62149524,62149535,62259450,62259459,62307645,62325686,62470947,62506916,63528051,66406223,66406245,66406261,66406271,67529080,63592565,67160305,67160316,67160341,67160345,67160347,67160362,67274251,70194680,63593656,64854124,67551125,67596488,67643586,67643600,67643614,67643628,75213963,75213996,61905080,63817262,64205490,64944725,64944738,64944751,66378271,66759156,22,64572354,64631762,75336027,75336297,75336471,74807721,64792267,64794726,65450790,65450809,65450896,66333933,64858827,67565798,69945741,73766959,73767049,73767115,73767510,64975618,75669260,65040105,67428493,65879385,65196601,65196604,65196605,65196606,68039954,68040073,68040115,65302433,65850658,65850673,66274728,66276361};
		List<Integer> l8 = new ArrayList<Integer>();
		for(Integer mediaId : mediaIds8){
			l8.add(mediaId);
		}
		
		int[] mediaIds9 = {66276370,74834083,74834171,74834201,74834241,74834291,74834354,74834395,74834538,74834598,74835532,65337986,65337988,65337989,65337990,65337991,65337992,65337993,65850942,65881262,65881267,65881272,65881277,65881282,65881287,65881292,65881297,65881302,65881307,65881312,68310062,68332854,74911907,74912040,65337995,65337997,65337998,65338000,65338001,65338002,65338003,65850988,65882368,65882373,65882378,65882383,65882388,65882393,65882398,75829398,75845412,75845575,76234605,65338004,65851648,65851684,65851685,65851686,65851813,65851843,65851872,65851895,65851917,65338006,65338008,65338010,65338011,65338012,65874880,65874901,65874977,65874998,65875019,65875040,65875061,65875082,65338013,65338015,65963730,65963733,70583780,65338016,70685072,70685076,70685091,70685103,72415535,72433212,65338020,65534628,65554016,65604907,65338018,65659673,72587634,72605951,72650996,74709627,74709807,65674276,65736689,65737363,65737366,65737369,65737372,65737375,65737378,65737381,65737384,65737387};
		List<Integer> l9 = new ArrayList<Integer>();
		for(Integer mediaId : mediaIds9){
			l9.add(mediaId);
		}
		
		int[] mediaIds10 = {65737390,65773569,65773585,65773678,65773714,65773843,66083607,66083608,66083609,66083622,66710880,65707021,65773893,65773895,65773901,67051047,67051052,65794928,65796339,73908735,74030147,65796340,65796335,65985101,65985102,65985103,76797952,66024159,66044592,66055819,66097555,66102654,66137767,77463679,77463786,62677964,65338022,66251776,66395527,66409945,66409952,66409953,71933405,71933426,71933441,71933455,66453829,66453820,66453763,66453803,66478667,66482243,66969501,66969504,66969507,66969510,66969513,66969516,66969519,66969522,66506289,77375180,66547927,67159789,67159825,67159835,67159838,67159871,67159874,67159876,67159898,67159929,67159938,67159943,72637275,72639549,72901029,66580210,66583633,66584426,66605440,67160020,67160042,67160071,67160076,67160101,67160108,67160147,67160158,67160159,67160162,67160188,67160205,67160246,70183281,66633452,73752125,73752137,66768586,72602322,67736420,67158341,67158459,67158499,67158504,67158517,67158530,67158548,67158569,67158574,67158580,67158612,67158613,67158614,67158618,67158624,67158625,67158626,67158627,67158633,67158634,67158643,67158658,70178075,70178156,70178579,70178702,70178818,70179118,71308307,71308315,71308509};
		List<Integer> l10 = new ArrayList<Integer>();
		for(Integer mediaId : mediaIds10){
			l10.add(mediaId);
		}
		
		int[] mediaIds11 = {71308552,71308608,71308642,71308657,71308664,71308787,71308856,71308890,67158751,67158782,70142883,67158855,67158868,67158897,67158900,67158925,67158933,67158974,67159044,67159084,67159099,67159100,67159111,67159139,67159140,67159141,67159193,67159214,67159233,67159273,67159312,67159341,67159360,67159387,67159394,67159454,67159455,67159477,67159481,67159498,67159500,67159519,70193971,70193981,70194005,70194017,70194043,70194050,67159570,67159594,67159619,67159639,72465152,67159661,67159683,67159690,67159702,67159703,67359839,163,65337999,65721879,66816650,67158392,67340591,67354125,67365772,68559245,67431261,72221593,72221630,67431324,67431382,72217459,72217770,72218008,72218102,72218199,67431437,67431452,67431531,72242828,72242874,67431797,72243527,72243577,72243629,72243678,67431831,72195744,72195853,72196028,72196066,67461770,67563780,67579821,67672792,67683881,67693687,67805875,67805883,67805891,67805899,67805907,67805915,67805923,67805931,67805939,67805947,67805955,67805963,67805979,67805987,67805995,67806003,67806019,67806027,67806035,67806043,67806051,67806059,67806067,67806083,67806091,67806099,67806107,67806115,67806123,67806143,67806151,67806159,67806167,67806175};
		List<Integer> l11 = new ArrayList<Integer>();
		for(Integer mediaId : mediaIds11){
			l11.add(mediaId);
		}
		
		int[] mediaIds12 = {67806183,67806191,67806199,67806207,67806215,67806223,67806231,67806239,67806255,67806264,67806272,67806280,67806288,67806296,67806308,67806317,67693692,68073856,68073864,68073870,68073874,68073878,68073882,68073890,68073894,68073898,68073906,68074823,68074827,68074831,68074835,68074839,68074843,68074847,68074851,68074855,68074859,68074863,68074867,68074875,68074880,68074888,68074892,68074896,68074900,68074904,68074908,68074916,68074923,68074927,68074931,68074935,68074939,68074943,68074947,68074952,68074956,68074960,68074964,68074968,68074972,68074976,68074980,68074984,68074988,68074992,68074996,68075005,68075009,68075013,68075022,71850153,71850203,71850253,71850303,71850376,71850426,71850476,71850547,71850615,71850674,71850724,71850788,71850853,71850903,71850953,71851003,71851053,71851103,71851166,71851234,71851290,71851346,71851444,71851514,71851669,71851735,71851799,74419469,74419520,74419571,74419623,74419676,67745571,70191162,70191169,70191184,70191193,70191216,70191241,70191270,70191286,70191299,70191345,70191363,70191402,70191411,70191418,70191436,70191456,70191468,70191479,70191507,70191527,70191548,70191597,70191604,70191619,70191630,70191650,67776760,67788426,67788584};
		List<Integer> l12 = new ArrayList<Integer>();
		for(Integer mediaId : mediaIds12){
			l12.add(mediaId);
		}
		
		int[] mediaIds13 = {69879017,67796331,67905775,67912168,67912197,67912453,67912471,67950430,67978748,67987385,67999220,67988000,67988036,67988117,68022716,68106126,68073886,68073902,68074871,68074912,68075038,68074884,68075026,68075030,68075043,68075034,67805971,67806011,67806247,67806325,72504923,67806075,68467080,68565634,68573928,68574573,68804326,69109052,69294988,69401711,69401814,69401871,69500513,69592775,69642032,69784290,70213927,69843346,70120660,70190484,70347554,70377409,70613996,72945395,72945405,72945432,72945479,72945584,72945600,72945628,74978656,70628267,70685034,70718900,77627190,71098342,71244998,73178776,71316704,71321832,71963847,72040872,72235782,72235832,72235917,71331604,72375211,72427386,72905164,73761833,72947128,72947184,72947201,72947160,72947199,72947395,72947396,72947400,72947478,72947529,72947530,72947614,72947616,72947624,72947802,72947809,72947812,72996147,72996162,73004815,73061075,73063418,73079429,73099344,73099372,73099403,73212162,73212550,73212799,73217399,73223455,73507212,74801728,77812797,73474405,73474610,73566981,73571162,74084976,74084991,74084997,72079854,73764305,75284108,75286077,75286108,75286132,75286173,75286185,75286211};
		List<Integer> l13 = new ArrayList<Integer>();
		for(Integer mediaId : mediaIds13){
			l13.add(mediaId);
		}
		
		int[] mediaIds14 = {75286241,75286289,75286344,75286365,73838089,75398136,77423959,73905637,76568336,76568785,73913296,74000395,74002079,74590657,74590766,74593744,74593895,74002394,74371744,74000105,74235244,75312461,75312570,75312665,75313030,75313046,75313071,75313117,74253324,74253634,74253677,74253719,74253764,74253806,74272248,74419103,77555016,77555471,74420758,74442634,74490584,74541955,74567250,74590739,75394982,74052206,74594869,74595311,74595403,74595440,74595313,74621788,74625235,74630644,74636212,74636301,74637427,74639139,74639397,74639579,74639628,74637660,74640890,74642843,74643363,74645836,74659244,74659428,74659441,74659467,74659510,74659528,74659534,74659601,74659606,74659658,74659697,74659748,74659753,74659794,74659873,74659927,74659960,74659982,74659994,74660030,74660073,74661677,74661691,74661731};
		List<Integer> l14 = new ArrayList<Integer>();
		for(Integer mediaId : mediaIds14){
			l14.add(mediaId);
		}
		
		int[] mediaIds15 = {74661751,74661777,74661803,74661815,74661831,74661856,74661879,74661914,74661923,74661942,74661967,74662001,74662027,74671215,74671283,74683708,74697880,74698734,74698844,74698880,74698884,74698773,74698835,74698853,74698903,74704012,74708079,74713977,74739486,74747655,74748569,74749592,74750408,74751287,74751817,74752209,74752535,74774067,74775324,74776775,74786859,74790362,74791637,74796013,74798491,74803881,74804650,74805428,74807744,74818208,74821348,74835252,74837163,74837615,74838031,74838310,74838425,74838451,74838452,74838453,74838709,74849263,74850040,74855872,74856839,74858270,74858333,74859164,74861046,74862190,74863293,74864241,74864884,74866583,74867270,74868103,74869043,74869488,74881587,74883901,74884476,74886154,74886732,74887270,74887374,74888085,74888599,74890653,74891546,74909601,74916572,74916755,74916901,77383098,74917045,74921211,74922363,74923870,74924429,74924867,74925875,74926384,74926877,74928050,74932127,74932703,74934419,74936838,74938660,74939591,74941393,74950499};
		List<Integer> l15 = new ArrayList<Integer>();
		for(Integer mediaId : mediaIds15){
			l15.add(mediaId);
		}
		
		int[] mediaIds16 = {74962402,74966064,74987580,74988384,74990935,74991072,74991777,74992049,74997168,74998045,75004735,75006320,75020595,75023538,75032719,75032776,75032807,75032831,75032857,75032881,75032913,75032944,75032990,75033009,75033055,75033102,75033121,75033162,75033169,75033186,75033209,75033237,75033302,75033346,75311748,77620747,75033219,75035675,75037348,75037407,75037422,75038292,75038851,75038866,75039779,75043923,75045300,75045329,75045336,75045352,75045422,75045843,75047177,75047287,75047316,75047488,75047499,75047609,75047619,75063268,75064566,75067901,75076032,75077406,75158180,75190140,75223710,75223721,75223827,75239526,75243813,75245507,75246495,75249495,75254557,75303535,75317257,75328026,75349698,75350448,75353536,75353850,75354056,75354146,75354475,75354540,75354641,75354734,75354824,75369788,75401129,75472826,75611397,75719017,75868382,75877662,75885329,67158807,76052084,75984761,76061832,76087186,76089277,76100067,76123363,76135929,76204709,76207613,76209052,76215744,76216408,76216699};
		List<Integer> l16 = new ArrayList<Integer>();
		for(Integer mediaId : mediaIds16){
			l16.add(mediaId);
		}
		
		int[] mediaIds17 = {76217392,76225285,76240138,76241488,76247819,76259765,76268165,76937651,76272665,65963736,76287940,76289167,76295833,76296903,76297152,76297195,76313108,76318792,76322396,76323658,76325936,76330598,76342816,76354126,76355114,76355957,76357043,76358382,76359221,76359759,76362377,76362810,76369064,76369473,76369956,76370442,77411334,76644631,76859109,76880590,76984196,77034877,77272573,77002118,77329827,77398619,77451493,77737203,77738835,77739241,77739427,77739505,77739720,77739805,77739886,77738667,77740168,77740281,77740319,77740346,77740353,77740378,77740404,77740528,73178594,74698037,74698129,76429778,76657481,76657564,76657614,76657666,76657718,76657751,76657800,76657895,77537226,77561790,77623827,77667332,77755797,77798123,77831696,77833893,77948949,77962336,77970497,78178704};
		List<Integer> l17 = new ArrayList<Integer>();
		for(Integer mediaId : mediaIds17){
			l17.add(mediaId);
		}
				
		List<Integer> total = new ArrayList<Integer>();
		total.addAll(l7);
		total.addAll(l8);
		total.addAll(l9);
		total.addAll(l10);
		total.addAll(l11);
		total.addAll(l12);
		total.addAll(l13);
		total.addAll(l14);
		total.addAll(l15);
		total.addAll(l16);
		total.addAll(l17);
		
		EntryObject eoInMedias = new PredicateBuilder().getEntryObject();
		//Predicate inMedias = eoInMedias.get("media_id").in(total.toArray(new Integer[]{}));
		Predicate inMedias = eoInMedias.get("media_id").in(74,80,67806143,67806151,67806264,67806272,67806288,67806296,67806308,71321832);
		pAnd = Predicates.and(pAnd, inMedias);
		return pAnd;
	}
	
	//This query takes around 14 seconds agains mysql...
	public static Predicate mediumQuery(){
		
		EntryObject eo = new PredicateBuilder().getEntryObject();
		
		Integer[] locality_ids = {59662769,66155333,59682815,59720152,61806254,62280790,62280797,62730498,63318664,64782719,65281056,65337798,65420079,65425412,65748692,66084392,66153697,66153766,66155333,66184048,66217306,66320625,66341524,66421885,66422047,66893770,66422047,66512533,66623857,66893637,66893770,66960581,67349922,67462796,67615527,67615725,67759697,68140098,68562509,68265492,68268003,68350153,69039196,69162526,69610160,70285498,70385432,70417658,71846940,71975964,72523078,73190398,73311988,73590600,73775732,73995221,75326500,75334184,75334201,77370267,78089039,78312649,78312655,78312661,78312691,78312703,78312708,78312877,78312955,78313027};
		List<Integer> locality_ids_list = new ArrayList<Integer>();
		for(Integer locality_id : locality_ids){
			locality_ids_list.add(locality_id);
		}
		
		Integer[] sponsorship_locality_ids = {59662769,59682815,59720152,61806254,62280790,62280797,62730498,63318664,64782719,65281056,65337798,65420079,65425412,65748692,66084392,66153697,66153766,66155333,66184048,66217306,66320625,66341524,66421885,66422047,66512533,66623857,66893637,66893770,66960581,67349922,67462796,67615527,67615725,67759697,68140098,68265492,68268003,68350153,68562509,69039196,69162526,69610160,70285498,70385432,70417658,71846940,71975964,72523078,73190398,73311988,73590600,73775732,73995221,75326500,77370267,78089039,78312649,78312655,78312661,78312691,78312703,78312708,78312877,78312955,78313027};
		List<Integer> sponsorship_locality_ids_list = new ArrayList<Integer>();
		for(Integer sponsorship_locality_id : sponsorship_locality_ids){
			sponsorship_locality_ids_list.add(sponsorship_locality_id);
		}
		
		Predicate pred = eo.isNot("disabled_for_upload")
			.and(
				eo.get("approval_advert_state").equal(11)
					.or(
						eo.isNot("inactive")
							.and(
								eo.get("content_approval_media_state").equal(18)
								.or(eo.get("clock_approval_media_state").equal(42))
								.or(eo.get("qc_approval_media_state").equal(22))
								.or(eo.get("num_instruction_rejected").greaterThan(0))
								)
						)
				)
			.and(eo.get("first_air_date").notEqual(0)
					.and(eo.get("delivery_deadline_ms_algo").greaterThan(1403560800000l)
							.or(eo.get("first_air_date").equal(0))
							.or(eo.get("delivery_deadline_ms_algo").lessThan(0).and(eo.get("first_air_date").greaterThan(1403560800000l)))
						)
				)
			.and(eo.get("locality_id").in(locality_ids).or(eo.get("sponsorship_locality_id").in(sponsorship_locality_ids)));
		
		/*disabled_for_upload = false 
				AND ( 
					approval_advert_state = 11 OR 
						( inactive = false 
							AND ( content_approval_media_state = 18 OR clock_approval_media_state = 42 OR qc_approval_media_state = 22 OR num_instruction_rejected > 0)
						)
					) 
				AND ( 
					first_air_date != 0 
						AND ( 
							delivery_deadline_ms_algo > 1403560800000 OR first_air_date = 0 
								OR ( 
									delivery_deadline_ms_algo < 0 
										AND first_air_date > 1403560800000
									)
							)
					) 
				AND ( 
						locality_id = 59662769 
						OR locality_id = 66155333 
						OR locality_id = 59682815 
						OR locality_id = 59720152 
						OR locality_id = 61806254 
						OR locality_id = 62280790 
						OR locality_id = 62280797 
						OR locality_id = 62730498 
						OR locality_id = 63318664 
						OR locality_id = 64782719 
						OR locality_id = 65281056 
						OR locality_id = 65337798 
						OR locality_id = 65420079 
						OR locality_id = 65425412 
						OR locality_id = 65748692 
						OR locality_id = 66084392 
						OR locality_id = 66153697 
						OR locality_id = 66153766 
						OR locality_id = 66155333 
						OR locality_id = 66184048 
						OR locality_id = 66217306 
						OR locality_id = 66320625 
						OR locality_id = 66341524 
						OR locality_id = 66421885 
						OR locality_id = 66422047 
						OR locality_id = 66893770 
						OR locality_id = 66422047 
						OR locality_id = 66512533 
						OR locality_id = 66623857 
						OR locality_id = 66893637 
						OR locality_id = 66893770 
						OR locality_id = 66960581 
						OR locality_id = 67349922 
						OR locality_id = 67462796 
						OR locality_id = 67615527 
						OR locality_id = 67615725 
						OR locality_id = 67759697 
						OR locality_id = 68140098 
						OR locality_id = 68562509 
						OR locality_id = 68265492 
						OR locality_id = 68268003 
						OR locality_id = 68350153 
						OR locality_id = 69039196 
						OR locality_id = 69162526 
						OR locality_id = 69610160 
						OR locality_id = 70285498 
						OR locality_id = 70385432 
						OR locality_id = 70417658 
						OR locality_id = 71846940 
						OR locality_id = 71975964 
						OR locality_id = 72523078 
						OR locality_id = 73190398 
						OR locality_id = 73311988 
						OR locality_id = 73590600 
						OR locality_id = 73775732 
						OR locality_id = 73995221 
						OR locality_id = 75326500 
						OR locality_id = 75334184 
						OR locality_id = 75334201 
						OR locality_id = 77370267 
						OR locality_id = 78089039 
						OR locality_id = 78312649 
						OR locality_id = 78312655 
						OR locality_id = 78312661 
						OR locality_id = 78312691 
						OR locality_id = 78312703 
						OR locality_id = 78312708 
						OR locality_id = 78312877 
						OR locality_id = 78312955
						OR locality_id = 78313027 
						OR sponsorship_locality_id = 59662769 
						OR sponsorship_locality_id = 59682815 
						OR sponsorship_locality_id = 59720152 
						OR sponsorship_locality_id = 61806254 
						OR sponsorship_locality_id = 62280790 
						OR sponsorship_locality_id = 62280797 
						OR sponsorship_locality_id = 62730498 
						OR sponsorship_locality_id = 63318664 
						OR sponsorship_locality_id = 64782719 
						OR sponsorship_locality_id = 65281056 
						OR sponsorship_locality_id = 65337798 
						OR sponsorship_locality_id = 65420079 
						OR sponsorship_locality_id = 65425412 
						OR sponsorship_locality_id = 65748692 
						OR sponsorship_locality_id = 66084392 
						OR sponsorship_locality_id = 66153697 
						OR sponsorship_locality_id = 66153766 
						OR sponsorship_locality_id = 66155333 
						OR sponsorship_locality_id = 66184048 
						OR sponsorship_locality_id = 66217306 
						OR sponsorship_locality_id = 66320625 
						OR sponsorship_locality_id = 66341524 
						OR sponsorship_locality_id = 66421885 
						OR sponsorship_locality_id = 66422047 
						OR sponsorship_locality_id = 66512533 
						OR sponsorship_locality_id = 66623857 
						OR sponsorship_locality_id = 66893637 
						OR sponsorship_locality_id = 66893770 
						OR sponsorship_locality_id = 66960581 
						OR sponsorship_locality_id = 67349922 
						OR sponsorship_locality_id = 67462796 
						OR sponsorship_locality_id = 67615527 
						OR sponsorship_locality_id = 67615725 
						OR sponsorship_locality_id = 67759697 
						OR sponsorship_locality_id = 68140098 
						OR sponsorship_locality_id = 68265492 
						OR sponsorship_locality_id = 68268003 
						OR sponsorship_locality_id = 68350153 
						OR sponsorship_locality_id = 68562509 
						OR sponsorship_locality_id = 69039196 
						OR sponsorship_locality_id = 69162526 
						OR sponsorship_locality_id = 69610160 
						OR sponsorship_locality_id = 70285498 
						OR sponsorship_locality_id = 70385432 
						OR sponsorship_locality_id = 70417658 
						OR sponsorship_locality_id = 71846940 
						OR sponsorship_locality_id = 71975964 
						OR sponsorship_locality_id = 72523078 
						OR sponsorship_locality_id = 73190398 
						OR sponsorship_locality_id = 73311988 
						OR sponsorship_locality_id = 73590600 
						OR sponsorship_locality_id = 73775732 
						OR sponsorship_locality_id = 73995221 
						OR sponsorship_locality_id = 75326500 
						OR sponsorship_locality_id = 77370267 
						OR sponsorship_locality_id = 78089039 
						OR sponsorship_locality_id = 78312649 
						OR sponsorship_locality_id = 78312655 
						OR sponsorship_locality_id = 78312661 
						OR sponsorship_locality_id = 78312691 
						OR sponsorship_locality_id = 78312703 
						OR sponsorship_locality_id = 78312708 
						OR sponsorship_locality_id = 78312877 
						OR sponsorship_locality_id = 78312955 
						OR sponsorship_locality_id = 78313027)*/
		
		return pred;
	}
	
}
