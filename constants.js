//commit policies
const AUTO_COMMIT_POLICY = "AUTO_COMMIT";
const MANUAL_COMMIT_POLICY = "MANUAL_COMMIT";
const COMMIT_POLICIES = [AUTO_COMMIT_POLICY, MANUAL_COMMIT_POLICY];
//internal topics
const TOPIC_STORE_NAME = "_topic_changelog";
const SUBSCRIPTIONS_STORE_NAME = "_subscriptions_changelog";
const COMMITS_STORE_NAME = "_commits_changelog";
//error codes
const ERROR_TOPIC_MISSING = "ETM";
const ERROR_TOPIC_ALREADY_EXISTS = "ETAE";

module.exports = {
    DEFAULT_DATA_PATH : "/var/famar/famarbroker/data/",
    //
    INDEX_FILE_SUFFIX : ".index",
    LOG_FILE_SUFFIX : ".log",
    //record constants
    OFFSET_LENGTH : 8,
    SIZE_LENGTH : 4,
    HEADER_LENGTH : 12,
    AUTO_COMMIT_POLICY,
    MANUAL_COMMIT_POLICY,
    COMMIT_POLICIES,
    TOPIC_STORE_NAME,
    SUBSCRIPTIONS_STORE_NAME,
    COMMITS_STORE_NAME,
    ERROR_TOPIC_MISSING,
    ERROR_TOPIC_ALREADY_EXISTS
}