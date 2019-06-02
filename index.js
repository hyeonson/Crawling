const request = require('request');
var moment = require('moment');
const cheerio = require('cheerio');
const convert = require('xml-js');
const qs = require('querystring');
const mysql = require('mysql');

exports.handler = async (event, context, callback) => {
    var connection = mysql.createConnection({
        host: 'hyeonsondb.c6tuwjcsobwr.ap-northeast-2.rds.amazonaws.com',
        user: 'hyeonson',
        password: '',
        database: 'ssgsag'
    });
    var companyName;
    var posterId;
    var requestUrl;
    var job;
    var logoUrlList;
    var companyUrl;
    function getHostIdx(companyName) {
        return new Promise(function (resolve, reject) {
    
            var requrl1 = 'http://www.catch.co.kr/apiGuide/guide/openAPIGuide/apiCompList?Service=1&CompName=';
            var requrl2 = '&SortCode=2&APIKey=OKi0USF3nvPj8a7RqbTErJqAeUNEt0YnkpKixpoEB2QcQ';
            var requestUrl = requrl1 + companyName + requrl2;
            var options = {
                headers: {
                    'Content-Type': 'Application/xml'
                },
                encoding: "utf-8",
                methods: "GET",
                uri: requestUrl
            }
            request.get(options, (err, response, body) => {
                if (err) {
                    resolve(err);
                }
                else {
                    if (response.statusCode == 200) {
                        var result = body;
                        var xmlToJson = convert.xml2json(result, { compact: true, spaces: 4 });
                        var jsonObject = JSON.parse(xmlToJson);
                        var data = jsonObject['Data'];
                        var companys = data['Companys'];
                        var company = companys['Company'];
                        var companySize;
    
                        if (company) {
                            if (company.constructor == Array) {
                                if (company[0]['CompSizeName']['_text'])
                                    companySize = company[0]['CompSizeName']['_text'];
                                else
                                    resolve(95000);
                            }
                            else {
                                if (company['CompSizeName']['_text'])
                                    companySize = company['CompSizeName']['_text'];
                                else
                                    resolve(95000);
                            }
                        }
    
                        if (companySize == '대기업') {
                            resolve(10000);
                        }
                        else if (companySize == '중견기업') {
                            resolve(20000);
                        }
                        else if (companySize == '중소기업') {
                            resolve(30000);
                        }
                        else {
                            resolve(95000);
                        }
                    }
    
                }
            });
        });
    }

    function alreadyReg(posterId) {
        return new Promise(function (resolve, reject) {
            connection.query('SELECT * FROM poster WHERE posterId = ?', posterId, function (err, rows, fields) {
                if (err) {
                    resolve(1);
                }
                else {
                    if (rows.length >= 1) {
                        resolve(1);
                    }
                    else {
                        resolve(0);
                    }
                }
            });
        });
    }

    function getJob(requestUrl) {
        return new Promise(function (resolve, reject) {
    
            request.get(requestUrl, (err, response, body) => {
                if (err) {
                    resolve(err);
                }
                else {
                    if (response.statusCode == 200) {
                        var result = body;
                        var xmlToJson = convert.xml2json(result, { compact: true, spaces: 4 });
                        var jsonObject = JSON.parse(xmlToJson);
                        var jobSearch = jsonObject['job-search'];
                        var jobs = jobSearch['jobs'];
                        job = jobs['job'];
                        resolve(job);
                    }
    
                }
            });
    
        });
    }

    function getLogoUrlList(job) {
        return new Promise(async function (resolve, reject) {
            var logoUrlList = [];
    
            for (var i = 0; i < job.length; i++) {
                var companyAttribs = job[i]['company']['name']['_attributes'];
                if (companyAttribs == null)
                    continue;
                var posterIdx;
                var categoryIdx = 4;
                posterId = job[i]['id']['_text'];
                //디비에서 같은 아이디값 검색해서 이미 있으면 continue
                if (await alreadyReg(posterId))
                    continue;
                companyUrl = companyAttribs['href'];
                companyName = job[i]['company']['name']['_cdata'];
                companyName = qs.escape(companyName);
                console.log(qs.unescape(companyName));
                var hostIdx = await getHostIdx(companyName);
                var posterStartDate = job[i]['posting-date']['_text'].substring(0, 10) + ' ' + job[i]['posting-date']['_text'].substring(11, 19);
                var posterEndDate = job[i]['expiration-date']['_text'].substring(0, 10) + ' ' + job[i]['expiration-date']['_text'].substring(11, 19);
                var publicOpenStartDate = posterStartDate;
                var publicOpenEndDate = posterEndDate;
                var documentDate = posterEndDate.substring(5, 16);
                var posterName = job[i]['position']['title']['_cdata'];
                var outline = job[i]['position']['industry']['_text'];
                var jobCategoryText = job[i]['position']['job-category']['_text'];
                var posterDetail = '사람인 웹사이트 참조';
                var posterWebsite = job[i]['url']['_text'];
                var interestStr = job[i]['position']['job-category']['_attributes']['code']; //'1002,101,119,202,209',  803: 유통무역상사 -> 8
                var interestIdx = interestStr.split(',');
                var keywordList = jobCategoryText.split(',');
                var keyword = '';
                if (typeof (keywordList) == 'string') {
                    keyword += '#' + keywordList;
                }
                else {
                    for (var j = 0; j < keywordList.length; j++) {
                        if (j == keywordList.length - 1) {
                            keyword += '#' + keywordList[j];
                        }
                        else {
                            keyword += '#' + keywordList[j] + ' ';
                        }
                    }
                }
    
                var target = '';
                target += job[i]['position']['experience-level']['_text'];
                target += ', ' + job[i]['position']['required-education-level']['_text'];
                var benefit = '';
                var locationCode = job[i]['position']['location']['_attributes']['code']; //지역코드, 101010, 강남구
    
                benefit += '근무지역: ' + job[i]['position']['location']['_cdata'];
                benefit += ', 연봉: ' + job[i]['salary']['_text'];
                benefit = benefit.replace(/&gt;/gi, '>');
                logoUrlList.push(benefit);
                var salaryCode = job[i]['salary']['_attributes']['code'];
    
                var imgSrc = await getImgSrc(companyUrl);
                var now = new Date();
                var posterRegDate = moment(now).format("YYYY-MM-DD HH:mm:ss");
                var isOnlyUniv = 0;
                var contentIdx = 0;
                var adminAccept = 0;
                logoUrlList.push(interestIdx);
    
                var params = [publicOpenStartDate, publicOpenEndDate, categoryIdx, imgSrc, posterName, posterRegDate, posterStartDate, posterEndDate, posterWebsite, isOnlyUniv, outline, target, benefit, documentDate, contentIdx, hostIdx, posterDetail, adminAccept, keyword, posterId];
    
                connection.query('INSERT INTO poster(publicOpenStartDate, publicOpenEndDate, categoryIdx, photoUrl, posterName, posterRegDate, posterStartDate, posterEndDate, posterWebsite, isOnlyUniv, outline, target, benefit, documentDate, contentIdx, hostIdx, posterDetail, adminAccept, keyword, posterId) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)', params, function (err, result) {
                    if (err) {
                        resolve(err);
                    }
                    else {
                        console.log('poster 삽입성공');
                        posterIdx = result.insertId;
                        var alreadyInput = [];
                        for (var j = 0; j < interestIdx.length; j++) {
                            interestIdx[j] = Math.floor(Number(interestIdx[j]) / 100) + 100;
                            var isAlready = false;
                            for(var k = 0; k < alreadyInput.length; k++)
                            {
                                if(alreadyInput[k] == interestIdx[j])
                                {
                                    isAlready = true;
                                    break;
                                }
                            }
                            if(isAlready == true)
                                continue;
                            alreadyInput.push(interestIdx[j]);
                            var params2 = [posterIdx, interestIdx[j]];
                            console.log('posterIdx: ' + posterIdx);
                            console.log('interestIdx: ' + interestIdx[j]);
                            connection.query('INSERT INTO poster_interest(posterIdx, interestIdx) VALUES (?, ?)', params2, function (err, result2) {
                                if (err) {
                                    resolve(err);
                                }
                                else {
                                    console.log('poster_interest 삽입성공');
                                }
                            });
                        }
    
    
                    }
                });
            }
            resolve(logoUrlList);
        });
    }

    function getImgSrc(companyUrl) {
        return new Promise(function (resolve, reject) {
    
            request(companyUrl, function (err2, response2, body2) {
                if(err2)
                {
                    resolve(err2);
                }
                const $ = cheerio.load(body2);
                //console.log(body2);
                $('.header_info > .title_info > .thumb_company > .inner_thumb > img').each(function () {
                    var imgTag = $(this);
                    var imgSrc = imgTag.attr('src');
                    resolve(imgSrc);
                })
            });
        });
    }

    const HOST = 'http://api.saramin.co.kr/job-search?stock=kospi+kosdaq&job_type=4&edu_lv=3&fields=posting-date+expiration-date+keyword-code+count&count=110';

    requestUrl = `${HOST}`;
    job = await getJob(requestUrl);
    logoUrlList = await getLogoUrlList(job);
}
