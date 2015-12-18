console.log('Loading function');

var Twitter = require('twitter');
var AWS = require("aws-sdk");
var dynamoDbDoc = new AWS.DynamoDB.DocumentClient();

var config = require('./config.json').development;
var client = new Twitter({
  consumer_key: config.consumer_key,
  consumer_secret: config.consumer_secret,
  access_token_key: config.access_token_key,
  access_token_secret: config.access_token_secret,
});

/**
 * Provide an event that contains the following key:
 *
 *   - operation: one of the operations in the switch statement below
 */
exports.handler = function(event, context) {
    console.log('Received event:', JSON.stringify(event, null, 2));

    var operation = event.operation;

    switch (operation) {
        case 'create':
            var status = event.status;
            if (!status) {
              return context.fail(new Error('Invalid or missing "status" parameter.'));
            }

            client.post('statuses/update', {status: status},  function(error, tweet, response){
              if(error) {
                console.error('Error posting tweet');
                console.error(error);
                return context.fail(new Error('Error posting tweet: ' + error));
              }

              console.info('Tweet posted');
              console.info(tweet);  // Tweet body.
              return context.succeed(tweet);
            });
            break;
        case 'ping':
            return context.succeed('pong');
        default:
            context.fail(new Error('Unrecognized operation "' + operation + '"'));
    }
};

/**
 * Schedule a tweet to be posted at a later time
 *
 *  - date: The time at which to post the tweet (in milliseconds since Jan 1 1970 UTC)
 *  - status: The tweet text
 */
exports.scheduledTweetPost = function(event, context) {
    var twoMinutes = 2*60*1000;

    /* GET PARAMETERS */
    var date = parseInt(event.date);
    var status = event.status;

    /* VALIDATE PARAMETERS */
    if (!status) { return context.fail(new Error('Invalid or missing "status" parameter.')); }
    if (!date) { return context.fail(new Error('Invalid or missing "date" parameter.')); }
    if (date < (new Date() - twoMinutes)) { return context.fail(new Error('Invalid date. Cannot post tweet in the past.')); }


    /* CONSTRUCT DYNAMODB ENTRY */
    var params = {
      TableName: 'scheduledTweets',
      Item: {
        // FIXME: Don't hard-code twitterAccount value
        "twitterAccount": "crc",
        "postedDate": date,
        "modifiedDate": new Date().valueOf(),
        "statusText": status,
        "isPosted": false
      },
      ReturnValues: 'ALL_OLD'
    };

    /* PUT ITEM IN DYNAMODB */
    dynamoDbDoc.put(params, function(err, data) {
      if (err) {
        console.error(err)
        return context.fail(new Error('Error scheduling tweet.'));
      }

      if (data && data.hasOwnProperty('Attributes')) {
        console.info('Tweet replaced.');
        console.info('Previous tweet: ')
        console.info(JSON.stringify(data, null, 2));
      } else {
        console.info('New tweet scheduled.')
      }
      return context.succeed(data);
    })
};

/**
 * Updates a previously scheduled but not posted tweet.
 *
 *  - oldDate: The previously scheduled time at which the tweet was to be posted (milliseconds since Jan 1 1970)
 *  - newDate: The updated time at which to post the tweet (milliseconds since Jan 1 1970 UTC)
 *  - status: The updated tweet text
 */
exports.scheduledTweetPut = function(event, context) {

};

/**
 * Deletes a previously scheduled but not posted tweet.
 *
 *  - date: The time at which the tweet was to be posted (milliseconds since Jan 1 1970)
 */
exports.scheduledTweetDelete = function(event, context) {

};

/**
 * Returns all scheduled tweets to post with a specified time range
 *
 *  - fromDate: The post date after which to return scheduled tweets (milliseconds since Jan 1 1970)
 *  - toDate: The post date before which to return scheduled tweets (milliseconds since Jan 1 1970)
 */
exports.scheduledTweetList = function(event, context) {
  /* GET PARAMETERS */
  var fromDate = parseInt(event.fromDate);
  var toDate = parseInt(event.toDate);
  var account = event.account;

  /* VALIDATE PARAMETERS */
  if (!fromDate) { return context.fail(new Error('Invalid or missing "fromDate" parameter.')); }
  if (!toDate) { return context.fail(new Error('Invalid or missing "toDate" parameter.')); }
  if (toDate < fromDate) { return context.fail(new Error('"toDate" cannot be before "fromDate".')); }
  if (!account) { return context.fail(new Error('Invalid or missing "account" parameter.')); }

  /* CONSTRUCT DYNAMODB ENTRY */
  var params = {
    TableName: 'scheduledTweets',
    KeyConditionExpression: 'twitterAccount = :account AND postedDate BETWEEN :from AND :to',
    ExpressionAttributeValues: {
      ':account': account,
      ':from': fromDate,
      ':to': toDate
    }
  };

  /* EXECUTE DYNAMODB QUERY */
  dynamoDbDoc.query(params, function(err, data) {
    if (err) {
      console.error(err)
      return context.fail(new Error('Error getting scheduled tweets.'));
    }

    console.info('Got list of scheduled tweets from DynamoDB.')
    return context.succeed(data);
  });
};

};
