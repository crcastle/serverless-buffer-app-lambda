console.log('Loading function');

var Promise = require('bluebird');
var Twitter = require('twitter');
var AWS = require('aws-sdk');
var dynamoDbDoc = new AWS.DynamoDB.DocumentClient();

var config = require('./config.json').development;
var client = new Twitter({
  consumer_key: config.consumer_key,
  consumer_secret: config.consumer_secret,
  access_token_key: config.access_token_key,
  access_token_secret: config.access_token_secret
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

    client.post('statuses/update', {status: status},  function(error, tweet/*, response*/){
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
      'twitterAccount': 'crc',
      'postedDate': date,
      'modifiedDate': new Date().valueOf(),
      'statusText': status,
      'isPosted': false
    },
    ReturnValues: 'ALL_OLD'
  };

  /* PUT ITEM IN DYNAMODB */
  dynamoDbDoc.put(params, function(err, data) {
    if (err) {
      console.error(err);
      return context.fail(new Error('Error scheduling tweet.'));
    }

    if (data && data.hasOwnProperty('Attributes')) {
      console.info('Tweet replaced.');
      console.info('Previous tweet: ');
      console.info(JSON.stringify(data, null, 2));
    } else {
      console.info('New tweet scheduled.');
    }
    return context.succeed(data);
  });
};

/**
 * Updates a previously scheduled but not posted tweet.
 *
 *  - oldDate: The previously scheduled time at which the tweet was to be posted (milliseconds since Jan 1 1970)
 *  - newDate: The updated time at which to post the tweet (milliseconds since Jan 1 1970 UTC)
 *  - status: The updated tweet text
 */
exports.scheduledTweetPut = function(/*event, context*/) {

};

/**
 * Deletes a previously scheduled but not posted tweet.
 *
 *  - date: The time at which the tweet was to be posted (milliseconds since Jan 1 1970)
 */
exports.scheduledTweetDelete = function(/*event, context*/) {

};

/**
 * Returns all scheduled tweets to post with a specified time range
 *
 *  Required:
 *  - account: Handle for the twitter account (without @)
 *
 *  Optional:
 *  - fromDate: The post date after which to return scheduled tweets (milliseconds since Jan 1 1970)
 *  - toDate: The post date before which to return scheduled tweets (milliseconds since Jan 1 1970)
 */
exports.scheduledTweetList = function(event, context) {
  /* GET PARAMETERS */
  var account = event.account;
  var fromDate = parseInt(event.fromDate) || null;
  var toDate = parseInt(event.toDate) || null;

  /* VALIDATE PARAMETERS */
  if (fromDate && toDate && toDate < fromDate) { return context.fail(new Error('"toDate" cannot be before "fromDate".')); }
  if (!account) { return context.fail(new Error('Invalid or missing "account" parameter.')); }

  /* CONSTRUCT DATE RANGE QUERY */
  var dateRangeQuery = '';
  var expAttrVals = { ':account': account };
  if (fromDate && toDate) {
    dateRangeQuery = ' AND postedDate BETWEEN :from AND :to';
    expAttrVals[':from'] = fromDate;
    expAttrVals[':to'] = toDate;
  } else if (fromDate) {
    dateRangeQuery = ' AND postedDate GT :from';
    expAttrVals[':from'] = fromDate;
  } else if (toDate) {
    dateRangeQuery = ' AND postedDate LT :to';
    expAttrVals[':to'] = toDate;
  }

  /* CONSTRUCT DYNAMODB QUERY */
  var params = {
    TableName: 'scheduledTweets',
    KeyConditionExpression: 'twitterAccount = :account' + dateRangeQuery,
    FilterExpression: 'isPosted <> :true',
    ExpressionAttributeValues: expAttrVals
  };

  /* EXECUTE DYNAMODB QUERY */
  dynamoDbDoc.query(params, function(err, data) {
    if (err) {
      console.error(err);
      return context.fail(new Error('Error getting scheduled tweets.'));
    }

    console.info('Got ' + data.Count + ' scheduled tweets from DynamoDB.');
    return context.succeed(data);
  });
};

/**
 * Checks DynamoDB for any tweets that are scheduled to be posted now and posts them to Twitter!
 * This is setup to be run every 5 minutes. It looks for tweets to be posted from the past
 * 7 minutes to the next 1 minute. It filters out tweets with `posted` set to true.
 */
exports.scheduledTweetWorker = function(event, context) {
  /* DEFINE DYNAMODB QUERY */
  var twitterAccount = 'crc';
  var now = new Date();
  var fromDate = +now - (7*60*1000); // now minus 7 minutes
  var toDate = +now + (1*60*1000); // now plus 1 minute

  var params = {
    TableName: 'scheduledTweets',
    KeyConditionExpression: 'twitterAccount = :account AND postedDate BETWEEN :from AND :to',
    FilterExpression: 'isPosted <> :true',
    ExpressionAttributeValues: {
      ':account': twitterAccount,
      ':from': fromDate,
      ':to': toDate,
      ':true': true
    }
  };

  /* EXECUTE DYNAMODB QUERY */
  dynamoDbDoc.query(params, function(err, data) {
    if (err) {
      console.error(err);
      return context.fail(new Error('Error getting scheduled tweets.'));
    }

    console.info('Worker got ' + data.Count + ' scheduled tweets from DynamoDB.');
    console.info(data);

    Promise.each(data.Items, function(scheduledTweet) {
      if (!scheduledTweet.isPosted) {
        return postAsync(scheduledTweet.statusText)
          .then(function(tweet) {
            console.log('Posted tweet: ' + scheduledTweet.statusText);
            return setTweetAsPosted(scheduledTweet, tweet.id_str);
          })
          .catch(function(error) {
            console.error('Error posting tweet: ' + scheduledTweet.statusText);
            console.error(error);
          });
      }
    })
    .then(function() {
      return context.succeed('Finished posting tweets. No errors.');
    })
    .catch(function(/*error*/) {
      return context.fail('Finished posting tweets. See error(s) above.');
    });
  });
};

function postAsync(tweetText) {
  return new Promise(function(resolve, reject) {
    client.post('statuses/update', {status: tweetText}, function(error, tweet/*, response*/) {
      if (error) {
        console.error('Error posting tweet.');
        reject(error);
      } else {
        console.log('Tweet posted.');
        resolve(tweet);
      }
    });
  });
}

function setTweetAsPosted(scheduledTweet, twitterId) {
  /* DEFINE DYNAMODB QUERY */
  var params = {
    TableName: 'scheduledTweets',
    Key: { 'twitterAccount': scheduledTweet.twitterAccount,
           'postedDate': scheduledTweet.postedDate },
    UpdateExpression: 'set #a = :boolVal, #b = :twitterId',
    ExpressionAttributeNames: { '#a': 'isPosted',
                                '#b': 'twitterId' },
    ExpressionAttributeValues: { ':boolVal': true,
                                 ':twitterId': twitterId }
  };

  /* EXECUTE DYNAMODB QUERY */
  return new Promise(function(resolve, reject) {
    dynamoDbDoc.update(params, function(err, data) {
      if (err) {
        console.error('Error setting tweet as posted (' + scheduledTweet.statusText + ')');
        reject(err);
      } else {
        resolve(data);
      }
    });
  });
}
