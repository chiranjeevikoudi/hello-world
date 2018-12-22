var MongoClient = require('mongodb').MongoClient;
var request = require("request");
var fs = require("fs");
var async = require("async");
var HTMLParser = require('node-html-parser');

var DATABASE = process.env.DATBASE?process.env.DATBASE:"name_gender_country";
var COLLECTION =  process.env.COLLECTION?process.env.COLLECTION:"babynamewizard_com";
var nameDBMongoURL = "mongodb://localhost:27017/"+DATABASE;
var nameLinksFile = "./name_links.json";
var nameURL = "http://www.babynamewizard.com/international-names-lists-popular-names-from-around-the-world";
var WRITE_TO = (process.env.WRITE_TO && (process.env.WRITE_TO==="DB" || process.env.WRITE_TO==="FILE"))?process.env.WRITE_TO:"FILE";
var FILE_NAME = process.env.FILE_NAME;

if(WRITE_TO === "FILE"){
    if(!(FILE_NAME && FILE_NAME.indexOf(".json")>-1 && FILE_NAME.length-5 === FILE_NAME.indexOf(".json") )){
        FILE_NAME = "name_gender_country.json";
    }
}


getNameLinks(function (err,nameLinks)  {
    if(err){
        console.log("error in getting name links",JSON.stringify(err));
    }
    else if(nameLinks){
        fs.writeFile(nameLinksFile,JSON.stringify(nameLinks),function (err) {
            if(err){
                console.log("error writing name links to file",JSON.stringify(err));
            }
            else{
                var namesWithCountryAndGender = [];
                async.each(nameLinks,function (nameLink,callback) {
                    var countryName = nameLink.country;
                    delete nameLink.country;
                    getNamesInCountry(countryName,nameLink,function (err,names) {
                        if(err){
                            callback(err);
                        }
                        else if(names){
                            namesWithCountryAndGender = namesWithCountryAndGender.concat(names);
                            callback(null);
                        }
                        else{
                            callback(null);
                        }
                    });
                },function (err) {
                    if(err){
                        console.log("error in getting names from links",JSON.stringify(err));
                    }
                    else if(namesWithCountryAndGender.length !== 0){
                        if(WRITE_TO === "DB"){
                            MongoClient.connect(nameDBMongoURL, function(err, nameDBConnection) {
                                if (err) throw err;
                                nameDBConnection.collection(COLLECTION).insert(namesWithCountryAndGender,function (err,res) {
                                    if(err){
                                        console.log("error inserting names data into database ",JSON.stringify(err));
                                    }
                                    else{
                                        console.log("names data successfully inserted into ",COLLECTION);
                                    }
                                    nameDBConnection.close();
                                });
                            });
                        }
                        else{
                            fs.writeFile(FILE_NAME,JSON.stringify(namesWithCountryAndGender),function (err) {
                                if(err){
                                    console.log("error writing names data to file ",JSON.stringify(err));
                                }
                                else{
                                    console.log("names data successfully written to ",FILE_NAME);
                                }
                            });
                        }
                    }
                    else{
                        console.log("names not found");
                    }
                });
            }
        });
    }
    else{
        console.log("links not found");
    }
});



function parseNameLinkDetails(nameData) {
    var body = HTMLParser.parse(nameData).querySelector("body");
    if(body){
        var namesLinks = [];

        if(body.querySelector('#node-38100') && body.querySelector('#node-38100').querySelector('.content')){
            var content = body.querySelector('#node-38100').querySelector('.content');
            var paragraphsWithLinks = content.querySelectorAll('p');
            for(var i=3;i<paragraphsWithLinks.length-1;i++){
                var countryNames = paragraphsWithLinks[i].text.split("\n").splice(1).map(function (countryName) {
                    return countryName.split(":")[0].trim();
                });
                var links = paragraphsWithLinks[i].querySelectorAll("a").map(function (linkTag) {
                    return linkTag.attributes["href"];
                });
                for(var countryIndex=0; countryIndex<countryNames.length; countryIndex++){
                    namesLinks.push({
                        "country":countryNames[countryIndex],
                        "male":links[countryIndex*2],
                        "female":links[(countryIndex*2)+1]
                    });
                }

            }
        }
        return namesLinks;
    }
    else{
        return null;
    }
}

function getNameLinks(cb){
    var options = {
        url:nameURL,
        method:"get",
        json:true
    };
    request(options,function (err,res,body) {
        if(err){
            cb(err);
        }
        else if(body){
            var links = parseNameLinkDetails(body);
            cb(null,links);
        }
        else {
            cb(null,null);
        }
    });
}

function getNamesInCountry(country,nameLinks,cb) {
    var namesWithCountryAndGender = [];
    async.eachOf(nameLinks,function (nameLinkValue,nameLinkKey,callback) {
        getNamesFromURL(nameLinkValue,function (err,names) {
            if(err){
                callback(err);
            }
            else if(names){
                names.forEach(function (name) {
                    namesWithCountryAndGender.push({
                        "cy":country,
                        "name":name,
                        "gen":nameLinkKey
                    });
                });
                callback(null);
            }
            else{
                callback(null);
            }
        });
    },function (err) {
        cb(err,namesWithCountryAndGender);
    })
}

function parseNamesDetails(nameData) {
    var body = HTMLParser.parse(nameData).querySelector("body");
    if(body && body.querySelector('.related-names') && body.querySelector('.related-names').querySelectorAll('li')){
        return body.querySelector('.related-names').querySelectorAll('li').map(function (nameElement) {
            return nameElement.text;
        });
    }
    else{
        return null;
    }
}

function getNamesFromURL(url,cb){
    var options = {
        url:url,
        method:"get",
        json:true
    };
    request(options,function (err,res,body) {
        if(err){
            cb(err);
        }
        else if(body){
            cb(null,parseNamesDetails(body));
        }
        else {
            cb(null,null);
        }
    });
}
