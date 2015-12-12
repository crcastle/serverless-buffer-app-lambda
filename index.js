console.log('Loading function');

var Twitter = require('twitter');
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
exports.scheduledTweetPost(event, context) {
    var twoMinutes = 2*60*1000;

    /* GET PARAMETERS */
    var date = parseInt(event.date);
    var status = event.status;

    /* VALIDATE PARAMETERS */
    if (!status) { return context.fail(new Error('Invalid or missing "status" parameter.')); }
    if (!date) { return context.fail(new Error('Invalid or missing "date" parameter.')) }
    if (date < (new Date() - twoMinutes)) { return context.fail(new Error('Cannot post tweet in the past.'))}


    /* CONSTRUCT DYNAMODB ENTRY */
    var params = {
      TableName = 'scheduled-tweets',
      Item: {
        'Date': date.toString(),
        'status': status,
        'posted': false
      }
    };

    /* PUT ITEM IN DYNAMODB */
    dynamoDbDoc.put(params, function(err, data) {
      if (err) {
        return context.fail(new Error('Error scheduling tweet: ', JSON.stringify(err, null, 2)));
      }

      console.info('Tweet scheduled: ', JSON.stringify(data, null, 2));
      return context.succeed(data);
    })
};

/**
 * Modifies a previously scheduled but not posted tweet.
 *
 *  - oldDate: The previously scheduled time at which the tweet was to be posted (milliseconds since Jan 1 1970)
 *  - newDate: The updated time at which to post the tweet (milliseconds since Jan 1 1970 UTC)
 *  - status: The updated tweet text
 */
exports.scheduledTweetPut(event, context) {

};

/**
 * Deletes a previously scheduled but not posted tweet.
 *
 *  - date: The time at which the tweet was to be posted (milliseconds since Jan 1 1970)
 */
exports.scheduledTweetDelete(event, context) {

};
