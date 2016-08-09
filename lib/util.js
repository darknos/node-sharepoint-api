/*jslint nomen: true*/
require("simple-errors");
var https           = require("https");
var url             = require("url");
var xpath           = require("xpath");
var cookie          = require("cookie");
var domParser       = new (require("xmldom").DOMParser)();
var constants       = require("constants");
var queryString     = require("querystring");
var wstrust         = require("wstrust");
var winston         = require("winston");
var baseConnector   = require("kido-connector");
var rekuest         = require("request");
var ntlm            = require("httpntlm/ntlm.js");
var fs              = require("fs");
var parseString     = require("xml2js").parseString;
var Agentkeepalive  = require("agentkeepalive");

var authenticationTypes = ["basic", "microsoft_online", "federation", "ntlm", "sso_idp"];
var versions = ["online", "2013", "2010"];

baseConnector.init("Sharepoint", winston);

// this class implements all features
var Util = function (settings) {
    "use strict";

    // Arguments validation
    if (!settings || typeof settings !== "object") throw new Error("'settings' argument must be an object instance.");
    if (!settings.host || typeof settings.host !== "string") throw new Error("'settings.host' property is a required string.");
    if (settings.port && typeof settings.port !== "number") throw new Error("'settings.port' property must be a number.");
    if (settings.timeout !== undefined && typeof settings.timeout !== "number") throw new Error("'settings.timeout' property must be a number.");
    if (settings.requestTimeout !== undefined && (typeof settings.requestTimeout !== "number" || settings.requestTimeout < 1000)) throw new Error("'settings.requestTimeout' property must be a number grater than 1000.");
    if (settings.username && typeof settings.username !== "string") throw new Error("'settings.username' property must be a string.");
    if (settings.password && typeof settings.password !== "string") throw new Error("'settings.password' property must be a string.");
    if (settings.strictSSL !== undefined && typeof settings.strictSSL !== "boolean") throw new Error("'settings.strictSSL' property must be a boolean.");

    settings.wsdl = settings.host + "/XRMServices/2011/Organization.svc?wsdl";

    if (settings.authType && typeof settings.authType !== "string") {
        throw new Error("'Authentication Type' property must be a string.");
    }

    if (!settings.authType) {
        // this is for backward compatibility
        // Sharepoint connector version <= 0.1.5 only have basic and MSOnline auth
        settings.authType = settings.useBasicAuth ? "basic" : "microsoft_online";
    }

    if (!settings.version) {
        settings.version = "online";
    }

    if ((typeof settings.version !== "string") || (versions.indexOf(settings.version) === -1)) { throw new Error("Sharepoint Version not supported"); }

    settings.nonHTTPS = settings.nonHTTPS || false;
    settings.strictSSL = settings.strictSSL || false;

    var self            = this,     // Auto reference
        entitySets      = null,     // String array containing all entity sets names
        pendingHook     = null,     // Function that will be executed after 'entitySets' array was populated.
        loginPath       = "/_forms/default.aspx?wa=wsignin1.0",                 // Login path for Sharepoint online and 2013
        loginEndpoint   = url.resolve("https://" + settings.host, loginPath),   // Login URL for the configured host
        buildPath,
        request,
        verifyAccess,
        buildODataId,
        getEntitySets,
        getAuthorization,
        clone,
        authenticateUser,
        _connector,
        addSecureOptions,
        RSTResponse = fs.readFileSync(__dirname + "/RequestSecurityTokenResponse.xml").toString(),

        isAuth = function (options, authType) {
            if (options && options.authType) {
                return options.authType === authType;
            }
            return settings.authType === authType;
        },

        isBasicAuth = function (options) {
            return isAuth(options, "basic");
        },

        isFederationAuth = function (options) {
            return isAuth(options, "federation");
        },

        isNTLMAuth = function (options) {
            return isAuth(options, "ntlm");
        },

        isSSO = function (options) {
            return isAuth(options, "sso_idp");
        },

        isMicrosoftOnline = function (options) {
            return isAuth(options, "microsoft_online");
        },

        isSharepoint2010 = function () {
            return settings.version === "2010";
        },

        createBaseConnector = function (settings, createSession) {
            var connector,
                credentialProps;

            if (isSSO()) credentialProps = ["token"];
            else credentialProps = ["username", "password"];

            connector = new baseConnector.Connector({
                config: settings,
                credentialProps: credentialProps,
                createSessionCb: createSession
            });

            return connector;
        },

        /*
        * Default callback function, it only throws an exception if an error was received.
        */
        defaultCb = function (err) {
            if (err) throw err;
        };

    settings.requestTimeout = settings.requestTimeout || 30000;

    buildPath = function (options) {
        var defaultRestEndpoint = isSharepoint2010() ? "/_vti_bin/listdata.svc/" : "/_api/",
            restEndpoint = options.restEndpoint || defaultRestEndpoint,
            path = (settings.site ? "/" + settings.site : "") + restEndpoint + options.command;

        while (path.indexOf("//") > -1) {
            path = path.replace("//", "/");
        }
        return encodeURI(path).replace(/\#/g, escape('#'));
    };

    addSecureOptions = function (reqOptions, nonHTTPS) {
        if (!nonHTTPS) {
            reqOptions.secureOptions = constants.SSL_OP_NO_TLSv1_2;
            reqOptions.ciphers = "ECDHE-RSA-AES256-SHA:AES256-SHA:RC4-SHA:RC4:HIGH:!MD5:!aNULL:!EDH:!AESGCM";
            reqOptions.honorCipherOrder = true;
            reqOptions.strictSSL = settings.strictSSL;
        }
    };

    // sends oData requests to the server
    request = function (options, cb) {
        var data,
            reqOptions,
            nonHTTPS = (isBasicAuth() || isNTLMAuth()) && settings.nonHTTPS,
            url,
            ntlmOptions,
            type1msg,
            agent,
            dataType = options.dataType ? options.dataType : options.headers && options.headers["content-type"] && (options.headers["content-type"].indexOf("xml") > -1) ? "xml" : "json",

            parseResponse = function (res, body) {
                var result = { statusCode: res.statusCode },
                    type = res.headers["Content-Type"] || res.headers["content-type"];

                try {
                    // returns parsed XML
                    if (type && type.indexOf("xml") > -1) result.data = domParser.parseFromString(body);
                    // returns parsed JSON
                    else if (type && type.indexOf("json") > -1) result.data = JSON.parse(body);
                    // returns plain body
                    else result.data = body;

                    return result;
                } catch (e) {
                    return cb(e);
                }
            },

            addAdditionalHeaders = function (item, reqOptions, data, options) {
                reqOptions.headers =  options.headers || {};

                if (!reqOptions.headers.accept) reqOptions.headers.accept = isSharepoint2010() ? "application/json" : "application/json;odata=verbose";

                if (data.length && dataType !== "binary") {
                    var defaultContentType = isSharepoint2010() ? "application/json" : "application/json;odata=verbose";
                    reqOptions.headers["content-type"] = (options.headers && options.headers["content-type"]) || defaultContentType;
                    reqOptions.headers["content-length"] = Buffer.byteLength(data);
                }

                if (isBasicAuth()) reqOptions.headers.Authorization = item.authHeader;
                else if (!isNTLMAuth()) reqOptions.headers.cookie = item.cookieAuthz;

                if (options.etag) reqOptions.headers["if-match"] = options.etag;

                if (options.digest) reqOptions.headers["X-RequestDigest"] = options.digest;
            };

        baseConnector.isHostAllowed(settings.host, function (err, allowed) {
            if (err) return cb(err);
            if (!allowed) return cb(new Error("The hostname is not allowed"));

            //Builds request options
            data = options.data && dataType === "json" ? JSON.stringify(options.data) : (options.data || "");

            url = nonHTTPS ? "http://" : "https://";
            url += settings.host + ":";
            url += settings.port || (settings.nonHTTPS ? 80 : 443);
            url += options.location || buildPath(options);

            if (isSSO()) options.password = options.token;

            _connector.getSession(options, function (err, item, auth) {
                if (err) return cb(err);
                if (isNTLMAuth(options)) {
                    ntlmOptions = {
                        username: options.username || item.username || settings.username,
                        password: options.password || item.password || settings.password,
                        workstation: options.workstation || settings.workstation || "",
                        domain: options.domain || settings.domain || ""
                    };

                    type1msg = ntlm.createType1Message(ntlmOptions);
                    agent = nonHTTPS ? new Agentkeepalive() : new Agentkeepalive.HttpsAgent();

                    reqOptions = {
                        method: options.method || "GET",
                        url: url,
                        headers: {
                            Authorization: type1msg,
                        },
                        agent: agent,
                        timeout: settings.requestTimeout
                    };

                    addSecureOptions(reqOptions, nonHTTPS);

                    rekuest(reqOptions, function (err, res) {
                        if (err) return cb(err);
                        if (!res.headers["www-authenticate"]) return cb(new Error("www-authenticate not found on response of second request"));

                        var type2msg = ntlm.parseType2Message(res.headers["www-authenticate"]),
                            type3msg = ntlm.createType3Message(type2msg, ntlmOptions);

                        reqOptions = {
                            method: options.method || "GET",
                            url: url,
                            body: data,
                            agent: agent,
                            timeout: settings.requestTimeout
                        };

                        addSecureOptions(reqOptions, nonHTTPS);
                        addAdditionalHeaders(null, reqOptions, data, options);

                        reqOptions.headers.Authorization = type3msg;

                        if (options.streaming) return cb(null, rekuest(reqOptions));

                        rekuest(reqOptions, function (err, res, body) {
                            if (err) return cb(err);
                            var result = parseResponse(res, body);
                            return cb(null, result);
                        });
                    });
                } else {
                    reqOptions = {
                        method: options.method || "GET",
                        url: url,
                        body: data,
                        timeout: settings.requestTimeout
                    };

                    addSecureOptions(reqOptions, nonHTTPS);
                    addAdditionalHeaders(item, reqOptions, data, options);

                    if (options.streaming) return cb(null, rekuest(reqOptions));
                    //console.log(reqOptions);
                    rekuest(reqOptions, function (err, res, body) {
                        if (err) return cb(err);

                        var result = parseResponse(res, body);
                        return cb(null, result);
                    });
                }
            });
        });
    };

    verifyAccess = function (options, cb) {
        var reqOptions = clone(options);
        reqOptions.command = isSharepoint2010() ? "Links" : "lists";

        request(reqOptions, function (err, results) {
            if (err || results.statusCode === 401 || results.statusCode === 403) {
                cb(err || new Error("Authentication fail."));
                return;
            }

            cb();
        });
    };

    // serialize the entity id
    buildODataId = function (id) {
        if (typeof id === "string") return "'" + id + "'";
        return String(id);
    };

    // retrives the entity sets from the service's metadata
    getEntitySets = function (options, cb) {
        // retrieves entity sets only once
        if (entitySets) return cb(null, entitySets);

        var odataOptions = clone(options);
        odataOptions.command = "$metadata";
        odataOptions.headers = {
            accept: "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8"
        };

        // gets the metadata XML
        self.oData(odataOptions, function (err, result) {
            try {
                // If server does not support $metadata, the entityset will be setted to an empty string
                if (err) entitySets = [];
                else
                    entitySets = xpath
                        .select("//*[local-name(.)='EntitySet']/@Name", result.data)
                        .map(function (attr) { return attr.value; });

                // adds methods dinamically
                if (pendingHook) pendingHook();

                // returns entities
                cb(null, entitySets);

            } catch (e) {
                // process error
                entitySets = null;
                cb(Error.create("Couldn't get entity sets. ", e));
            }
        });
    };

    // does the HTTP POST and returns the authorization data
    getAuthorization = function (postData, cb) {
        var options = {
            method: "POST",
            host: settings.host,
            port: settings.port || 443,
            path: loginPath,
            headers: {
                "User-Agent": "Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.1; Win64; x64; Trident/5.0)",
                "Content-Type": "application/x-www-form-urlencoded",
                "Content-Length": Buffer.byteLength(postData)
            }
        },
            req;

        addSecureOptions(options, settings.nonHTTPS);
        req = https.request(options, function (res) {
            res.setEncoding("utf8");

            res.once("data", function () {
                if (!res.headers["set-cookie"]) return cb(Error.create("No cookies found"), {statusCode: res.statusCode});

                var cookies = cookie.parse(res.headers["set-cookie"].join(";"));

                cb(null, {
                    FedAuth: cookies.FedAuth,
                    rtFa: cookies.rtFa
                });
            });
        });

        req.once("error", function (error) {
            return cb(error);
        });

        req.write(postData);
        req.end();
    };

    // Reeturns a new copy of an object or value
    clone =  function (source) {
        // is source null or a value type?
        if (source === null || typeof source !== "object") return source;

        // returns a copy of an array
        if (source instanceof Array) return source.map(clone);

        // returns a copy of a date
        if (source instanceof Date) return new Date(source.getTime());

        // returns a copy of an object
        var result = {};
        Object.keys(source).map(function (prop) { result[prop] = clone(source[prop]); });
        return result;
    };

    authenticateUser = function (credential, cb) {
        var session,
            msOnlineOptions,
            secureBaseUrl,
            rstresponse,
            assertion,
            created,
            expires,
            reqOptions,

            authenticationCB = function (err, postData) {
                if (err) return cb(err);

                reqOptions = {
                    uri: secureBaseUrl + "/_trust/",
                    method: "POST",
                    body: postData,
                    headers: {
                        "Content-Type": "application/x-www-form-urlencoded"
                    }
                };

                addSecureOptions(reqOptions);
                rekuest(reqOptions, function (err, response) {
                    if (err) return cb(err);
                    if (!response.headers["set-cookie"]) return cb(Error.create("Error authentication with IP token", response));

                    var cookies = cookie.parse(response.headers["set-cookie"].join(";"));

                    session = {
                        cookieAuthz: "FedAuth=" + cookies.FedAuth,
                        username: credential.username
                    };

                    cb(null, session);
                });
            };

        secureBaseUrl = "https://";
        secureBaseUrl += settings.host + ":";
        secureBaseUrl += settings.port || 443;

        if (isBasicAuth()) {
            // creates cache item (session)
            session = {
                authHeader: "Basic " + new Buffer(credential.username + ":" + credential.password).toString("base64"),
                username: credential.username,
                password: credential.password
            };

            cb(null, session);
        } else if (isFederationAuth()) {
            settings.tokenType = settings.tokenType || "urn:oasis:names:tc:SAML:1.0:assertion";
            settings.keyType = settings.keyType || "http://schemas.xmlsoap.org/ws/2005/05/identity/NoProofKey";

            wstrust.authenticateUsingWSTrust(settings, function (err, response) {
                if (err) return cb(err);

                var postDataObject = {
                    wa: "wsignin1.0",
                    wresult: response.rawResponse
                },
                    postData = queryString.stringify(postDataObject);

                return authenticationCB(null, postData);
            });
        } else if (isNTLMAuth()) {
            session = {
                username: credential.username,
                password: credential.password
            };

            cb(null, session);

        } else if (isSSO()) {
            assertion = credential.token.replace('\"', '"');

            parseString(assertion, {explicitArray: false, explicitRoot: true}, function (err, jsondata) {
                if (err) return cb(err);

                var root = Object.keys(jsondata)[0],
                    nodePrefix = root.indexOf(":") > -1 ? root.substring(0, root.indexOf(":")) + ":" : "",
                    conditionsNode = nodePrefix + "Conditions",
                    postDataObject,
                    postData;

                created = jsondata[root][conditionsNode].$.NotBefore;
                expires = jsondata[root][conditionsNode].$.NotOnOrAfter;

                rstresponse = RSTResponse.replace("{created}", created)
                    .replace("{expires}", expires)
                    .replace("{assertion}", assertion);

                postDataObject = {
                    wa: "wsignin1.0",
                    wresult: rstresponse,
                    wctx: secureBaseUrl + "/_layouts/15/Authenticate.aspx?Source=%2F"
                };

                postData = queryString.stringify(postDataObject);
                return authenticationCB(null, postData);
            });
        } else {
            //default is "microsoft_online"
            msOnlineOptions = {
                username: credential.username,
                password: credential.password,
                appliesTo: loginEndpoint,
            };

            wstrust.authenticateUsingMSOnline(msOnlineOptions, function (err, response) {
                if (err) return cb(err);

                getAuthorization(response.token.toString(), function (err, authz) {
                    if (err) return cb(err);

                    // creates cache item (session)
                    session = {
                        expires: response.expires,
                        authz: authz,
                        cookieAuthz: "FedAuth=" + authz.FedAuth + ";rtFa=" + authz.rtFa,
                        username: credential.username,
                        password: credential.password
                    };

                    cb(null, session);
                });
            });
        }
    };

    _connector = createBaseConnector(settings, authenticateUser);

    this.authenticate = function (options, cb) {
        baseConnector.isHostAllowed(settings.host, function (err, allowed) {
            if (err) return cb(err);
            if (!allowed) return cb(new Error("The hostname is not allowed"));

            if (isSSO()) options.password = options.token;
            _connector.getSession(options, function (err, session, auth) {
                if (err) return cb(err);

                if (isMicrosoftOnline())
                    // populates entitySets array
                    getEntitySets(options, function (err) {
                        // validates error
                        if (err) return cb(err);
                        cb(null, { auth: auth, user: session.username });
                    });

                else if (isNTLMAuth()) cb(null, { auth: auth, user: session.username });
                else
                    // Verifies that credentials are valid and have access rights.
                    verifyAccess(options, function (err) {
                        if (err) return cb(err);

                        cb(null, { auth: auth, user: session.username });
                    });
            });
        });
    };

    // Executes an oData Command
    this.oData = function (options, cb) {
        var requestOptions,

            needsDigest = function (method) {
                var verbs = ["POST", "PUT", "UPDATE", "PATCH", "DELETE"];
                return (verbs.indexOf(method) > -1) && !isSharepoint2010();
            };

        // handles optional 'options' argument
        if (!cb && typeof options === "function") {
            cb = options;
            options = {};
        }

        // sets default values
        cb = cb || defaultCb;
        options = options || {};
        if (!options || typeof options !== "object") return cb(new Error("'options' argument is missing or invalid."));
        if (!options.command || typeof options.command !== "string") return cb(new Error("'options.command' argument is missing or invalid."));

        if (options.forceDigestRequest || needsDigest(options.method)) {
            requestOptions = {
                auth: options.auth,
                username: options.username,
                password: options.password,
                method: "POST"
            };

            if (isSharepoint2010()) {
                requestOptions.restEndpoint = "/_vti_bin/";
                requestOptions.command = "sites.asmx";
                requestOptions.data = '<?xml version="1.0" encoding="utf-8"?><soap:Envelope xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xmlns:xsd="http://www.w3.org/2001/XMLSchema" xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/">  <soap:Body>    <GetUpdatedFormDigest xmlns="http://schemas.microsoft.com/sharepoint/soap/" />  </soap:Body></soap:Envelope>';
                requestOptions.headers = {
                    "SOAPAction": "http://schemas.microsoft.com/sharepoint/soap/GetUpdatedFormDigest",
                    "content-type": "text/xml",
                    "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8"
                };
            } else {
                requestOptions.data = {};
                requestOptions.command = "/contextinfo";
            }

            request(requestOptions, function (err, d) {
                if (err) return cb(err);
                var digest;
                if (isSharepoint2010()) digest = xpath.select("//GetUpdatedFormDigestResult/text()", d.data);
                else digest = d && d.data && d.data.d && d.data.d.GetContextWebInformation && d.data.d.GetContextWebInformation.FormDigestValue;

                if (!digest) return cb(Error.create("Unable to retrieve Form Digest"));

                options.digest = digest;
                request(options, cb);
            });
            return;
        }
        request(options, cb);
    };

    // Returns an string array with the name of all entity sets
    this.entitySets = function (options, cb) {
        // handles optional 'options' argument
        if (!cb && typeof options === "function") {
            cb = options;
            options = {};
        }

        // sets default values
        cb = cb || defaultCb;
        options = options || {};
        if (!options || typeof options !== "object") return cb(new Error("'options' argument is missing or invalid."));

        getEntitySets(options, cb);
    };

    // gets an entity by its id
    this.get = function (options, cb) {
        // handles optional 'options' argument
        if (!cb && typeof options === "function") {
            cb = options;
            options = {};
        }

        // sets default values
        cb = cb || defaultCb;
        options = options || {};
        if (!options.resource || typeof options.resource !== "string") return cb(new Error("'resource' property is missing or invalid."));
        if (options.id === null || options.id === undefined) return cb(new Error("'id' property is missing."));

        self.oData({
            auth: options.auth,
            username: options.username,
            password: options.password,
            token: options.token,
            method: "GET",
            command: "/" + options.resource + "(" + buildODataId(options.id) + ")"
        }, cb);
    };

    // executes a query on an entity set
    this.query = function (options, cb) {
        // handles optional 'options' argument
        if (!cb && typeof options === "function") {
            cb = options;
            options = {};
        }

        // sets default values
        cb = cb || defaultCb;
        options = options || {};
        if (!options.resource || typeof options.resource !== "string") return cb(new Error("'resource' property is missing or invalid."));

        if (!_.has(options, "top")) options.top = "5000";

        var err = null,
            params = ["filter", "expand", "select", "orderBy", "top", "skip"]
                .map(function (prop) {
                    if (options[prop]) {
                        var value = options[prop];
                        if (typeof value !== "string") {
                            err = new Error("The property '" + prop + "' must be a valid string.");
                            return null;
                        }
                        return "$" + prop.toLowerCase() + "=" + value;
                    }
                    return null;
                })
                .filter(function (param) { return !!param; });

        if (err) return cb(err);

        params.push("$inlinecount=" + (options.inLineCount ? "allpages" : "none"));

        self.oData({
            auth: options.auth,
            username: options.username,
            password: options.password,
            token: options.token,
            method: "GET",
            command: "/" + options.resource + (params.length === 0 ? "" : "?" + params.join("&"))
        }, cb);
    };

    this.download = function (options, cb) {
        // handles optional 'options' argument
        if (!cb && typeof options === "function") {
            cb = options;
            options = {};
        }

        // sets default values
        cb = cb || defaultCb;
        options = options || {};
        if (!options.resource || typeof options.resource !== "string") return cb(new Error("'resource' property is missing or invalid."));

        self.oData({
            auth: options.auth,
            username: options.username,
            password: options.password,
            token: options.token,
            method: "GET",
            comman: "/" + options.resource + "/File"
        }, function (err, data) {
            if (err) return cb(err);

            var file = data && data.data && data.data.d && data.data.d,
                loc,
                reqOptions;

            if (!file || !file.ServerRelativeUrl) return (new Error("File location not found."));
            loc = file.ServerRelativeUrl;

            reqOptions = {
                auth: options.auth,
                username: options.username,
                password: options.password,
                token: options.token,
                method: "GET",
                streaming: true,
                command: loc,
                location: loc
            };
            self.oData(reqOptions, function (err, stream) {
                if (err) return err;
                cb(null, stream, {"Content-Disposition": "attachment;filename="   + file.Name});
            });
        });
    };

    this.processQuery = function (options, cb) {
        // handles optional 'options' argument
        if (!cb && typeof options === "function") {
            cb = options;
            options = {};
        }

        // sets default values
        cb = cb || defaultCb;
        options = options || {};

        var odataOptions = {
            auth: options.auth,
            username: options.username,
            password: options.password,
            token: options.token,
            method: "POST",
            data: options.data
        };

        odataOptions.headers = {
            "content-type": "text/xml"
        };

        if (isSharepoint2010()) {
            odataOptions.restEndpoint = "/_vti_bin/client.svc";
            odataOptions.forceDigestRequest = true;
        }

        odataOptions.command = "/ProcessQuery";

        self.oData(odataOptions, cb);
    };

    // gets all links of an entity instance to entities of a specific type
    this.links = function (options, cb) {
        // handles optional 'options' argument
        if (!cb && typeof options === "function") {
            cb = options;
            options = {};
        }

        // sets default values
        cb = cb || defaultCb;
        options = options || {};
        if (options.id === null || options.id === undefined) return cb(new Error("'id' property is missing."));
        if (!options.resource || typeof options.resource !== "string") return cb(new Error("'resource' property is missing or invalid."));
        if (!options.entity || typeof options.entity !== "string") return cb(new Error("'entity' property is missing or invalid."));

        self.oData({
            auth: options.auth,
            username: options.username,
            password: options.password,
            token: options.token,
            method: "GET",
            command: "/" + options.resource + "(" + buildODataId(options.id) + ")/$links/" + options.entity
        }, cb);
    };

    // returns the number of elements of an entity set
    this.count = function (options, cb) {
        // handles optional 'options' argument
        if (!cb && typeof options === "function") {
            cb = options;
            options = {};
        }

        // sets default values
        cb = cb || defaultCb;
        options = options || {};
        if (!options.resource || typeof options.resource !== "string") return cb(new Error("'resource' property is missing or invalid."));

        self.oData({
            auth: options.auth,
            username: options.username,
            password: options.password,
            token: options.token,
            method: "GET",
            command: "/" + options.resource + "/$count"
        }, cb);
    };

    // adds an entity instance to an entity set
    this.create = function (options, cb) {
        // handles optional 'options' argument
        if (!cb && typeof options === "function") {
            cb = options;
            options = {};
        }

        // sets default values
        cb = cb || defaultCb;
        options = options || {};
        if (options.data === null || options.data === undefined) return cb(new Error("'data' property is missing."));
        if (!options.resource || typeof options.resource !== "string") return cb(new Error("'resource' property is missing or invalid."));

        self.oData({
            auth: options.auth,
            username: options.username,
            password: options.password,
            token: options.token,
            data: options.data,
            method: "POST",
            command: "/" + options.resource
        }, cb);
    };

    // does a partial update of an existing entity instance
    this.replace = function (options, cb) {
        // handles optional 'options' argument
        if (!cb && typeof options === "function") {
            cb = options;
            options = {};
        }

        // sets default values
        cb = cb || defaultCb;
        options = options || {};
        if (options.id === null || options.id === undefined) return cb(new Error("'id' property is missing."));
        if (options.data === null || options.data === undefined) return cb(new Error("'data' property is missing."));
        if (!options.resource || typeof options.resource !== "string") return cb(new Error("'resource' property is missing or invalid."));

        self.oData({
            auth: options.auth,
            username: options.username,
            password: options.password,
            token: options.token,
            data: options.data,
            method: "PUT",
            command: "/" + options.resource + "(" + buildODataId(options.id) + ")"+(options.select? "?$select="+options.select : ""),
            etag: "*"
        }, cb);
    };

    // does a complete update of an existing entity instance
    this.update = function (options, cb) {
        // handles optional 'options' argument
        if (!cb && typeof options === "function") {
            cb = options;
            options = {};
        }

        // sets default values
        cb = cb || defaultCb;
        options = options || {};
        if (options.id === null || options.id === undefined) return cb(new Error("'id' property is missing."));
        if (options.data === null || options.data === undefined) return cb(new Error("'data' property is missing."));
        if (!options.resource || typeof options.resource !== "string") return cb(new Error("'resource' property is missing or invalid."));

        self.oData({
            auth: options.auth,
            username: options.username,
            password: options.password,
            token: options.token,
            data: options.data,
            method: isSharepoint2010() ? "MERGE" : "PATCH",
            command: "/" + options.resource + "(" + buildODataId(options.id) + ")"+(options.select? "?$select="+options.select : ""),
            etag: "*"
        }, cb);
    };

    // removes an existing instance from an entity set
    this.remove = function (options, cb) {
        // handles optional 'options' argument
        if (!cb && typeof options === "function") {
            cb = options;
            options = {};
        }

        // sets default values
        cb = cb || defaultCb;
        options = options || {};
        if (!options.resource || typeof options.resource !== "string") return cb(new Error("'resource' property is missing or invalid."));
        if (options.id === null || options.id === undefined) return cb(new Error("'id' property is missing."));

        self.oData({
            auth: options.auth,
            username: options.username,
            password: options.password,
            token: options.token,
            method: "DELETE",
            command: "/" + options.resource + "(" + buildODataId(options.id) + ")",
            etag: "*"
        }, cb);
    };

    // adds methods dynamically to the instance passed by parameter.
    // for each entity set, methods for get, update, create, etc. will be added.
    this.hook = function (target) {
        // function for add a single method
        var addMethod = function (method, prefix, entitySet) {

            target[prefix + entitySet] = function (options, cb) {

                if (!cb && typeof options === "function") {
                    cb = options;
                    options = {};
                }

                cb = cb || defaultCb;
                options = options || {};
                options.resource = entitySet;

                method(options, cb);
            };
        },

            // function for add all methods to a every entity set
            addAllMethods = function (entitySets) {
                entitySets.forEach(function (entitySet) {
                    addMethod(self.get, "get", entitySet);
                    addMethod(self.query, "query", entitySet);
                    addMethod(self.links, "links", entitySet);
                    addMethod(self.count, "count", entitySet);
                    addMethod(self.create, "create", entitySet);
                    addMethod(self.replace, "replace", entitySet);
                    addMethod(self.update, "update", entitySet);
                    addMethod(self.remove, "remove", entitySet);
                });
            };

        // adds te methods if the array of entity sets was populated
        if (entitySets) addAllMethods(entitySets);
        else
            // wait for the entitySets
            pendingHook = function () {
                addAllMethods(entitySets);
                pendingHook = null;
            };
    };

    // returns
    this.lookupMethod = function (connectorInstance, name, cb) {
        var method,
            wrapper;

        if (!pendingHook) {
            method = connectorInstance[name];
            if (typeof method === "function") return cb(null, method);
            return cb();
        }

        // returns a wrappers that waits until entity sets were retrieved
        wrapper = function (options, callback) {
            // forces the invocation to entity sets
            self.entitySets(options, function (err, result) {
                if (err) return callback(err);

                // after entitySets were retrieved, invokes the method or returns not found (404)
                method = connectorInstance[name];
                if (typeof method === "function") return method(options, callback);
                return callback(null, null);
            });
        };

        return cb(null, wrapper);
    };
};

module.exports = Util;
