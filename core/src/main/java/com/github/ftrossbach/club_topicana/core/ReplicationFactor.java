package com.github.ftrossbach.club_topicana.core;

/**
 * Created by ftr on 10.11.17.
 */
public interface ReplicationFactor {

    boolean isSpecified();

    int count();

    static ReplicationFactor of(int count){

        return new SpecifiedCount(count);

    }

    static ReplicationFactor ignore(){
        return new UnspecifiedCount();
    }


    class SpecifiedCount implements ReplicationFactor {

        private final int count;

        private SpecifiedCount(int count) {
            if(count <= 0){
                throw new IllegalArgumentException("Count must be larger than 0");
            }
            this.count = count;
        }

        @Override
        public boolean isSpecified() {
            return true;
        }

        @Override
        public int count() {
            return count;
        }
    }

    class UnspecifiedCount implements ReplicationFactor {


        private UnspecifiedCount(){

        }


        @Override
        public boolean isSpecified() {
            return false;
        }

        @Override
        public int count() {
            throw new IllegalStateException("Must not call count() on an unspecified PartitionCount");
        }
    }


}


