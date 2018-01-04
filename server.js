const ws = require('ws');
const fs = require('fs');
const https = require('https');
const url = require('url');
const crypto = require('crypto');
const db = require('./db.js');


// Couple of utility function for use with promise chains

function stopHere( handler ) {
    return function(error) {
        handler(error);
        return { then: function() {} };
    }
}

function logError( error ) {
    console.log(error);
}

// Connect to the database
var DB = new db ({
    connectionLimit : 100, //important
    host     : 'localhost',
//    user     : 'voicerail',
//    password : 'dod89Nkj$ddh282*pSKLD8)wlq76gdfS%skw2l',
    user     : 'voicerailAdmin',
    password : 'kdiu7d8wldpDh987453w1tSk87298E)(8u2nlDP90398slbxci7e8t&',
    database : 'voicerail',
});


// Get the instance ID for this server...
var gccInstanceId;
const http = require('http');
var res = http.request({
    method      : 'GET',
    hostname    : 'metadata',
    path        : '/computeMetadata/v1/instance/id',
    headers     : { 'Metadata-Flavor':'Google' }
}, function(response){
    var str = '';
    response.on('data', function (chunk) { str += chunk; });
    response.on('end', function () { gccInstanceId = str; });
    response.on('error', function (details) { console.log(details); process.exit(1); });
}).end();
require('deasync').loopWhile(function(){return !gccInstanceId});
console.log('GCC Instance ID = '+gccInstanceId);

// Convert the instance ID into an internal instance Id
var instanceId;
DB.getValue('SELECT id FROM websocketServer WHERE gccInstanceId=?',gccInstanceId).then(function(id){
    if (id) instanceId=id;
    else {
        DB.insert('websocketServer',{gccInstanceId:gccInstanceId}).then(function(id){
            instanceId=id;
            console.log('xxx'+id);
        });
    }
});

// Wait until we have got the instance ID
require('deasync').loopWhile(function(){return !instanceId;});

console.log('Got websocketServer ID :'+instanceId);

// Check if there is a table for messages for this instance - if not create one
var messageQueueTableExists;
var messageQueueTable = 'instanceMessage_'+instanceId;

DB.selectRow(messageQueueTable,{ id:1 }).then(function(result){
    console.log(result); 
    messageQueueTableExists = true;
}).catch(function(error) {
    if ( error.code=='ER_NO_SUCH_TABLE' ) {
        // Create the table
        DB.exec(`
            CREATE TABLE ?? (
                id          INT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
                hubId    INT UNSIGNED NOT NULL,
                createdAt   INT UNSIGNED NOT NULL,
                message     TEXT,
                INDEX (createdAt)
            )
        `,messageQueueTable).then(function(result){
            return DB.insert(messageQueueTable,{ id: 1, createdAt: new Date().getTime()/1000 });
        }).then(function(result){
            return DB.selectRow(messageQueueTable,{ id:1 }).then(function(result){
                if (!result) {
                    console.log('Unexpected error trying to add flag row into instanceMessage table.');
                    process.exit();
                } else {
                    messageQueueTableExists = true;
                }
            });
        }).catch(function(error){
            console.log('Unexpected error trying to create instanceMessage table...');
            console.log(error);
            process.exit();
        });
    } else {
        // An unexpected error occurred - there is nothing we can do now except die
        console.log('Unexpected error trying to find instanceMessage table...');
        console.log(error);
        process.exit();
    }
});

// Authenticate websocket connections
function doAuthentication( connectionInfo, authenticationResultCallback ) {

    console.log('Authenticating conection...');

    let authFailure = function(reason) {
        console.log('Refusing access: '+reason);
        return authenticationResultCallback(false,403,reason);
    } 

    // Parse out the requested URL
    let req = connectionInfo.req;
    req.foo='bar';
    let urlParts = url.parse(req.url, true);
    let query = urlParts.query;

    // Extract and check the timestamp
    if (!query.time.length) return authFailure('Could not find "time" parameter - The request must include a timestamp.');

    let now = new Date().getTime()/1000;
    if (Math.abs(query.time-now) > 300) return authFailure('The request is too old or out of sync - Check your clock and try again.');

    // Extract the signature from the parameters
    if (!query.sig.length) return authFailure('Could not find "sig" parameter - The request must be signed.');
    let sig = query.sig;
    delete query.sig;

    // Get the hub ID from the GET string
    if (!query.hubId.length) return authFailure('Could not find "hubId" parameter - This is a mandatory parameter.');
    console.log('Connection attempt from hub: '+query.hubId); 

    DB.getRow('SELECT hub.secret, hub.id AS hubId FROM hub WHERE externalId=?',query.hubId)
    .catch(stopHere(function(error){
        return authFailure('Unexpected problem looking up your hub ID - Try again later.');
    })).then(function(rowData){
    
        if (!rowData) return authFailure('Invalid hub ID - Please check it and try again.');   

        var { secret, hubId } = rowData;
        console.log('External hub ID mapped to internal ID: '+hubId); 

        // Build a query string to generate the signature from
        let toSign = req.method+' '+urlParts.pathname+'?';
        // the parameters must be in alphabetical order
        let sortedKeys = Object.keys(query).sort(function (a, b) {
            return a.toLowerCase().localeCompare(b.toLowerCase());
        });
        for( i=0; i<sortedKeys.length; i++ ) {
            toSign += encodeURIComponent(sortedKeys[i]) + '=' + encodeURIComponent(query[sortedKeys[i]]) + '&';
        }
        toSign = toSign.slice(0,-1);

        // Generate the HMAC
        let hmac = crypto.createHmac('sha256', secret);
        hmac.update(toSign);
        let desiredSig = hmac.digest('hex');

        // Compare the desired HMAC with what we were actually given
        // Protect against timing attacks
        sig = sig.padEnd(desiredSig.length,' ');
        let mismatch=0;
        for (i=0; i<desiredSig.length; i++) {
             mismatch |= (sig.charCodeAt(i) ^ desiredSig.charCodeAt(i));
        }
      
        // Send our decision 
        if (mismatch) { 
            authFailure('Request signature invalid - please check you are using the correct ID and secret');
        } else {
            // TODO - before updating the instance ID for this hub see what the old instance ID was
            // If it has changed then grab any old jobs off the queue for the old instance

            // Update the database to let everyone know that hubId is connected to us
            DB.insert('hubConnection',{hubId:hubId, instanceId:instanceId},'REPLACE').then(function(result){

                // AUTHENTICATION SUCCEEDED
                console.log('Hub '+hubId+' authenticated');
                req.hubId=hubId;
                authenticationResultCallback(true);

            }).catch(function(){
                return authFailure('There was an unexpected problem registering your connection in the database');
            });
        }
            
    }).catch(function(){
        return authFailure('Unexpected error');
    });

}

const httpsServer = https.createServer({
    cert: fs.readFileSync('/etc/sslmate/www.voicerail.com.chained.crt'),
    key: fs.readFileSync('/etc/sslmate/www.voicerail.com.key')
});
httpsServer.listen(8080);

const wss = new ws.Server({
    verifyClient: doAuthentication,
    server: httpsServer
});

console.log('Listening for connections');

var hubs = {};


wss.on('connection', function connection(ws,req) {

    console.log('Accepted connection from hub ID: '+req.hubId);
    var hubId=req.hubId;
    // we're done with the request now...
    req='';

    // Initialise some of our own state variables
    ws.lastPing = 0;
    ws.lastPong = 0;
    ws.sending = 0;

    hubs[hubId] = ws;
    
    ws.on('message', function incoming(message) {
        // We don't accept any communication from the hub so disregard these
        console.log('Received message from hub %d: %s',hubId, message);
    });

    ws.on('pong', function () {
        console.log('Pong from hub %d',hubId);
        ws.lastPong = now;
    });

    ws.on('timeout', function () {
        console.log('Timeout');
        closeConnection(hubId);
    });

});

var lastEventTimes = {
    messageQueueKeepaliveUpdate: 0,
    hubPingCheck: 0,
    
}
var intervals = {
    messageQueueKeepaliveUpdate: 10,
    hubPing: 10,
    hubPingCheck: 5
}

var timeouts = {
    hubPing:    40,
}

var now;
var maxMessageId = 1;
var outstandingMessages = 0;
var maxOutstandingMessages = 100;
var messagesPerPoll = 100;

function timeFor( event ) {
    if ((now-lastEventTimes[event]) > intervals[event]*1000) {
        lastEventTimes[event] = now;
        return true;
    }
    return false;
}

function closeConnection( hubId ) {
    var hub = hubs[hubId];
    if (hub.sending) outstandingMessages--;
    delete(hubs[hubId]);
    console.log('Closing connection to hub %d',hubId);
    hub.close();
    // TODO - UPDATE THE DATABASE HERE
}

function sendMessage( messageData ) {

    var { id, hubId, message } = messageData;
    delete messageData.id;
    if (!hubs[hubId]) {
        console.log('Hub %d is no longer connected - putting their message on the unclaimed message queue',hubId);
        // This hub is no longer here - put the message back on the unclaimed message queue
        DB.insert('instanceMessage_unclaimed',messageData).then(function(){
            return DB.delete(messageQueueTable,{id:id});
        }).catch(function(error){
            console.log('Problem adding message to unclaimed queue:');
            console.log(error);
        });
        return;
    }
    console.log('Got message %d for hub %d: %s', id, hubId, message);
    var hub = hubs[hubId];
    // if we're already sendng this message then ignore this
    if (hub.sending >=id) {
        console.log('Already sending message %d to hub %d',id,hubId);
        return;
    }
    hub.sending=id;
    outstandingMessages++;
    hub.send(message+'\n',function(error) {
        hub.sending=0;
        outstandingMessages--;
        if (error) {
            console.log('Error sending message %d to hub %d - putting message on the unclaimed message queue',id,hubId);
            DB.insert('instanceMessage_unclaimed',messageData).catch(function(error){
                console.log('Problem adding message to unclaimed queue:');
                console.log(error);
            });
            closeConnection(hubId);
        } else {
            console.log('Message %d successfully sent to hub %d',id,hubId);
            DB.delete(messageQueueTable,{id:id}).catch(function(error){
                console.log('Problem deleting message from queue:');
                console.log(error);
            });
        }
    });
}

function poll() {
    now = new Date().getTime();
    pollStart = now;

    // Update the messageQueueKeepalive if its time to do that
    if (timeFor('messageQueueKeepaliveUpdate')) {
        console.log('Updating keepalive row in %s',messageQueueTable);
        DB.update(messageQueueTable,{id:1},{createdAt:pollStart/1000}).catch(logError);
        lastMessageQueueKeepaliveUpdateTime = pollStart;
    }

    // Check through all attached hubs to see if any have timed out, or if any are due for pinging
    if (timeFor('hubPingCheck')) {
        console.log('Checking for hubs that need pinging and for unresponsive hubs');
        for (hubId in hubs) {
            let hub = hubs[hubId];
            // First see if we've had a response to the last ping
            if ((hub.lastPing > hub.lastPong)) {
                // if not see if this is overdue
                if ((now - hub.lastPing) > (timeouts.hubPing * 1000)) {
                    console.log('Connection to hub %d timed out - pong was late',hubId);
                    closeConnection(hubId);
                }
            // OK... so the last ping has been ponged - is it time for a new ping yet?
            } else if ((now - hub.lastPing) > intervals.hubPing * 1000) {
                console.log('Pinging hub %d',hubId);
                hub.lastPing = now;
                hub.ping('',undefined,true);
            }
        }
    }

    // Get any messages we need to send.
    if ( outstandingMessages < maxOutstandingMessages ) {
        DB.getRows('SELECT * FROM ?? WHERE id>? ORDER BY id ASC LIMIT ?',messageQueueTable,maxMessageId,messagesPerPoll).then(function(rows){
            if (!rows) return;
            maxMessageId = rows[rows.length-1].id;
            for (var i=0; i<rows.length; i++) {
               
                sendMessage( rows[i] );
            }
        }).catch(function(error){
            console.log('Problem getting messages that need sending from database:');
            console.log(error);
        });
    }

    /*
    for (var hubId in hubs) {
        hubs[hubId].send('something',function(error) {
            if (error) {
                closeConnection(hubId);
            } else {
                console.log('CALLBACK');
            }
        });
    }
    */


    let wait = 1000 - (new Date().getTime() - pollStart);
    if (wait<0) wait=0;
    console.log('Waiting %d ms. %d hub(s) connected, %d messages being sent',wait,Object.keys(hubs).length,outstandingMessages);
    setTimeout(poll,wait);    
}

poll();
