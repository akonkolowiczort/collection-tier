package uy.edu.ort.arqrealtime.collection.tier;

final class RSVPProducerFactory {

    static RSVPProducer getInstance(final String... args) {
        if(args != null && args.length > 0 && args[0].equalsIgnoreCase("kafka")){
            return new RSVPKafkaProducer();
        } else {
            // returning default
            return new RSVPConsoleProducer();
        }
    }
}
