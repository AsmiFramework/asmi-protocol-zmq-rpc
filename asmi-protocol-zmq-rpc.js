var config = require('config');
var zmq = require('zmq');
var uuid = require('uuid');
var util = require('util');
var shortid = require('shortid32');

function AsmiProtocolZMQRPC(modName) {
	this.modName = modName;
	this.pendingCallbacks = {};
}

var p = AsmiProtocolZMQRPC.prototype;

p.initServer = function (outsideInterface) {
	this.outsideInterface = outsideInterface;
	this.mod = require(this.modName);

	this.conf = config.modules[this.modName];
	this.serverPort = this.conf.port;

	var startServer = false;

	if (this.conf.protocolOpts && this.conf.protocolOpts.startServer) {
		startServer = this.conf.protocolOpts.startServer;
	} else {
		startServer = true;
	}

	this.initClient();

	if (startServer) {
		this.startServer();
	}
};

p.startServer = function () {
	var root = this;

	try {
		// master process  create ROUTER and DEALER sockets, bind endpoints
		var router = zmq.socket('router');
		var dealer = zmq.socket('dealer');

		router.bindSync('tcp://*:' + this.serverPort);
		dealer.bindSync('tcp://*:' + (this.serverPort + 1000));

		console.log("ZMQ PROXY STARTED - PORT: " + this.serverPort + " - " + root.modName);

		// forward messages between router and dealer
		router.on('message', function () {
			var frames = Array.apply(null, arguments);
			dealer.send(frames);
		});

		dealer.on('message', function () {
			var frames = Array.apply(null, arguments);
			router.send(frames);
		});

		root.initWorker();
	} catch (e) {
		// console.log(this.modName);
		// console.log(util.inspect(e));
	};
};

p.initClient = function () {
	var root = this;
	var reservedMethods = ["getInstanceID"];

	// create request endpoint
	var requester = zmq.socket('dealer');

	// handle replies from responder
	requester.on("message", function (data) {
		var response;
		response = JSON.parse(Array.apply(null, arguments)[1].toString());

		// console.log("Received response:", response);
		if (root.pendingCallbacks[response.requestID]) {
			// console.log(response.requestID);
			var pendingCallback = root.pendingCallbacks[response.requestID];
			if (pendingCallback) {
				pendingCallback[response.callbackID].apply(null, response.data);
				if (root.conf.protocolOpts) {
					if (!root.conf.protocolOpts.retainPendingCallbacks) {
						delete root.pendingCallbacks[response.requestID];
					}
				} else {
					delete root.pendingCallbacks[response.requestID];
				}
			} else {
				console.warn("couldn't find callback details");
			}
		} else {
			console.warn("couldn't find caller details");
		}
	});

	requester.connect("tcp://localhost:" + this.serverPort);

	for (var method in this.mod) {
		if (reservedMethods.indexOf(method) == -1) {
			// need not do for properties
			if (typeof root.mod[method] == 'function') {
				root.outsideInterface[method] = (function (name) {
					return function () {
						var requestID = name + "-" + uuid.v1();

						var args = [];
						var callbacks = {};

						// generate callbackIDs instead of passing callback functions in the message. Won't work as we are sending a JSON message
						for (var key in arguments) {
							if (typeof arguments[key] == "function") {
								var callbackID = "callback." + shortid.generate();
								callbacks[callbackID] = arguments[key];
								args.push(callbackID);
							} else {
								args.push(arguments[key]);
							}
						}

						root.pendingCallbacks[requestID] = callbacks;

						var message = {
							requestID: requestID,
							callee: name,
							args: args
						};
						requester.send(JSON.stringify(message));
					};
				})(method);
			}
		}
	}
};

p.initWorker = function () {
	var root = this;

	try {
		root.mod.start();
	} catch (e) {};

	var responder = zmq.socket('dealer');

	this.setupSocketMonitoring(responder);
	responder.connect("tcp://localhost:" + (this.serverPort + 1000));

	console.log("ZMQ WORKER STARTED - PORT: " + this.serverPort + " - " + root.modName);

	responder.on("message", function () {
		var args = Array.apply(null, arguments);

		// parse incoming message
		var requestData = JSON.parse(args[1]);

		var callee = root.mod[requestData.callee];
		var requestArgs = [];
		for (var i = 0; i < requestData.args.length; i++) {
			var arg = requestData.args[i];
			if (typeof arg == 'string' && arg.indexOf("callback.") == 0) {
				requestArgs.push((function (callbackID) {
					return function () {
						responder.send([args[0], '', JSON.stringify({
							requestID: requestData.requestID,
							callbackID: callbackID,
							data: Array.prototype.slice.call(arguments)
						})]);
					};
				})(arg));
			} else {
				requestArgs.push(arg);
			}
		}
		callee.apply(root.mod, requestArgs);
	});
};

p.setupSocketMonitoring = function (socket) {
	/*socket.on('connect', function (fd, ep) {console.log('connect, endpoint:', ep);});
	socket.on('connect_delay', function (fd, ep) {console.log('connect_delay, endpoint:', ep);});
	socket.on('connect_retry', function (fd, ep) {console.log('connect_retry, endpoint:', ep);});
	socket.on('listen', function (fd, ep) {console.log('listen, endpoint:', ep);});
	socket.on('bind_error', function (fd, ep) {console.log('bind_error, endpoint:', ep);});
	socket.on('accept', function (fd, ep) {console.log('accept, endpoint:', ep);});
	socket.on('accept_error', function (fd, ep) {console.log('accept_error, endpoint:', ep);});
	socket.on('close', function (fd, ep) {console.log('close, endpoint:', ep);});
	socket.on('close_error', function (fd, ep) {console.log('close_error, endpoint:', ep);});
	socket.on('disconnect', function (fd, ep) {console.log('disconnect, endpoint:', ep);});*/

	// Handle monitor error
	/*socket.on('monitor_error', function (err) {
		console.log('Error in monitoring: %s, will restart monitoring in 5 seconds', err);
		setTimeout(function () { socket.monitor(); }, 5000);
	});

	socket.monitor();*/
};

module.exports = exports = function (modName) {
	return new AsmiProtocolZMQRPC(modName);
};
