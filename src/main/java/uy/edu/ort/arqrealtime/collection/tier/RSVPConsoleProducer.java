package uy.edu.ort.arqrealtime.collection.tier;

final public class RSVPConsoleProducer implements RSVPProducer {

    public void sendMessage(final String messageKey, final byte[] message) {
        final String stringMessage = new String(message);
        System.out.println("Producing message with key= " + messageKey + ", and value= " + stringMessage);
        logMessage(messageKey, stringMessage);
    }

    public void close() {
        System.out.println("Closing producer");
    }

    private void logMessage(final String messageKey, final String message){
        if(message.contains("error")){
            HybridMessageLogger.moveToFailed(messageKey);
        } else {
            try {
                HybridMessageLogger.removeEvent(messageKey);
            } catch (Exception e) {
                //this should be logged...
            }
        }
    }

}
