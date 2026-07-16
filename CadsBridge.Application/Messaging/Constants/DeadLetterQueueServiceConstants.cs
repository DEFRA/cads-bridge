namespace CadsBridge.Application.Messaging.Constants;

public static class DeadLetterQueueServiceConstants
{
    public const string StringDataType = "String";
    public const string NumberDataType = "Number";
    public const string AllAttributes = "All";

    public static class Tags
    {
        public const string DeadLetterQueue = "Dead Letter Queue";
        public const string MainQueue = "Main Queue";
    }

    public static class SqsAttributes
    {
        public const string ApproximateNumberOfMessages = "ApproximateNumberOfMessages";
        public const string ApproximateNumberOfMessagesNotVisible = "ApproximateNumberOfMessagesNotVisible";
        public const string ApproximateNumberOfMessagesDelayed = "ApproximateNumberOfMessagesDelayed";
        public const string ApproximateReceiveCount = "ApproximateReceiveCount";
    }

    public static class MessageAttributes
    {
        public const string CorrelationId = "CorrelationId";
        public const string Subject = "Subject";
        public const string DlqOriginalMessageId = "DLQ_OriginalMessageId";
        public const string DlqFailureReason = "DLQ_FailureReason";
        public const string DlqFailureMessage = "DLQ_FailureMessage";
        public const string DlqFailureTimestamp = "DLQ_FailureTimestamp";
        public const string DlqReceiveCount = "DLQ_ReceiveCount";
        public const string DlqPrefix = "DLQ_";
    }

    public static class Timeouts
    {
        public const int ReceiveMessageVisibilitySeconds = 60;
    }

    public static class Limits
    {
        public const int MaxSqsMessageAttributeLength = 256;
        public const int MaxSqsReceiveMessages = 10;
    }

    public static class LogMessages
    {
        public const string NoDlqConfigured = "No DLQ configured for message {MessageId}";
        public const string SentToDlq = "Message {OriginalMessageId} sent to DLQ with new ID {DlqMessageId}";
        public const string MovedToDlq = "Message {MessageId} successfully moved to DLQ";
        public const string SendSucceededDeleteFailed = "CRITICAL: Sent to DLQ but DELETE FAILED - DUPLICATE WILL OCCUR";
        public const string FailedToSend = "Failed to send to DLQ - message will retry";
        public const string DeleteFailed = "Send succeeded but delete failed for MessageId: {MessageId}";
    }
}