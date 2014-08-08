package com.craftedbytes.utils;

import com.craftedbytes.domain.QuickSearch;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Random;

public class QuickSearchFactory {
	
	final static Random random = new Random(System.currentTimeMillis());
	final ArrayList<QuickSearch> qs_list = new ArrayList<QuickSearch>();
	
	public Collection<QuickSearch> createQuickSearchList(int size) {
		for(int i = 0; i < size; i ++ ) {
			qs_list.add(createQuickSearchObject());
		}
		return qs_list;
	}
	
	public QuickSearch createQuickSearchObject() {
		int i = random.nextInt(14);
		long l = random.nextLong();
		
		return new QuickSearch(l * 2, 
												  "l-name" + i,
												  l * 3,
												  "d-name" + i,
												  l * 4,
												  "b-name" + i,
												  l * 5,
												  "m-name" + i,
												  random.nextInt(4),//media_content_approval_instruction
												  i,
												  random.nextInt(32),//media_clock_approval_instruction
												  i * 3,
												  random.nextInt(52),//media_instruction_approval_instruction
												  10,//media_house_number_instruction
												  true,
												  i * 3,
												  l * 3,
												  l * 2,
												  l * 2,
												  l * 2,
												  l * 1,
												  "copy_code" + i,
												  random.nextBoolean(),//aired
												  l * 1,
												  i,
												  l * 3,
												  i * 3,
												  "title" + i,
												  random.nextBoolean(),
												  random.nextBoolean(),
												  l * 2,
												  l * 2,
												  l * 3,
												  l * 3,
												  i * 3,
												  i * 3,
												  random.nextBoolean(),
												  l * 3,
												  random.nextBoolean(),
												  "media_agency_planning" + i,
												  "media_agency_buying" + i,
												  "creative_agency" + i,
												  "production_agency" + i,
												  "post_production_agency" + i,
												  "other_agency" + i,
												  random.nextBoolean(),
												  "media_house_nr" + i,
												  "b_p_code" +i,
												  random.nextBoolean(),
												  random.nextBoolean(),
												  l * 3,
												  l * 3,
												  l * 2,
												 random.nextBoolean(),//inactive
												 l * 2,
												 i * 3,
												 l * 2,
												 l * 3,
												 l * 3,
												 l * 2,
												 10,//approval_advert_state
												 12,//quality_check_state
												 14,
												 l * 3,
												 random.nextBoolean(),
												l * 3,
												i * 2,
												l * 2,
												l * 3,
												l * 3,
												l * 2,
												l,
												l * 2,
												 l * 3,
												 l * 2,
												 random.nextInt(20),//content_approval_media_state
												 l * 2,
												 l * 3,
												 i * 3,
												 l * 3,
												 l * 3,
												 i * 2,
												 random.nextInt(42),//clock_approval_media_state
												 l * 3,
												 i * 3,
												 i * 2,
												 random.nextInt(4), //num_instruction_rejected
												 random.nextInt(2),//num_instruction_approval_not_performed
												 l * 3,
												 i * 2,
												 random.nextBoolean(),
												 i * 3);
	}

}
