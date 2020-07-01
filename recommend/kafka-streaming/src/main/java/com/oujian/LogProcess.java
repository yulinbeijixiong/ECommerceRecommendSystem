package com.oujian;

import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;

public class LogProcess implements Processor<byte[],byte[]> {
    private ProcessorContext context;
    @Override
    public void init(ProcessorContext processorContext) {
        this.context=processorContext;
    }

    @Override
    public void process(byte[] bytes, byte[] bytes2) {
        String line = new String(bytes2);
        if(line.contains("PRODUCT_RATING_PREFIX:")){
            String trim = line.split("PRODUCT_RATING_PREFIX:")[1].trim();
            context.forward("logProcessor".getBytes(),trim.getBytes());
        }

    }


    @Override
    public void punctuate(long l) {

    }

    @Override
    public void close() {

    }
}
