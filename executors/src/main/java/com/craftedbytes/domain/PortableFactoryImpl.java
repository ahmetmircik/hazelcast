package com.craftedbytes.domain;

import com.hazelcast.nio.serialization.Portable;
import com.hazelcast.nio.serialization.PortableFactory;

public class PortableFactoryImpl  implements PortableFactory {
		public final static int FACTORY_ID = 2;

		@Override
		public Portable create(int classId) {
			switch(classId) {
			case 3 : return new QuickSearch();
			default : return null; 
			}
		}
}
	
