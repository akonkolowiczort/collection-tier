package uy.edu.ort.arqrealtime.collection.tier;

interface RSVPProducer {
    void sendMessage(String messageKey, byte[] message);
    void close();

}
