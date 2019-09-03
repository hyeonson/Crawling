const request = require('request');
const moment = require('moment');
const cheerio = require('cheerio');
const convert = require('xml-js');
const qs = require('querystring');
const mysql = require('mysql');

process.env.TZ = 'Asia/Seoul';

const connection = mysql.createConnection({
    host: 'hyeonsondb.c6tuwjcsobwr.ap-northeast-2.rds.amazonaws.com',
    user: 'hyeonson',
    password: '',
    database: 'ssgsag'
});

function getHostIdx(...args) {
    let companyName = args[0];
    if(companyName == null) {
        return 95000;
    }
    return new Promise(function (resolve, reject) {

        const requrl1 = 'http://www.catch.co.kr/apiGuide/guide/openAPIGuide/apiCompList?Service=1&CompName=';
        const requrl2 = '&SortCode=2&APIKey=OKi0USF3nvPj8a7RqbTErJqAeUNEt0YnkpKixpoEB2QcQ';
        let requestUrl = requrl1 + companyName + requrl2;
        const options = {
            headers: {
                'Content-Type': 'Application/xml'
            },
            encoding: "utf-8",
            methods: "GET",
            uri: requestUrl
        }
        request.get(options, (err, response, body) => {
            if (err) {
                console.log(err);
                resolve(95000);
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
                    else{
                        resolve(95000);
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

function alreadyReg(...args) {
    //null인 경우는 없다고 가정
    let posterId = args[0];
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


function getJob(...args) {
    let requestUrl = args[0];
    return new Promise(function (resolve, reject) {
        /*
        const options = {
            headers: {
                'Content-Type': 'Application/xml'
            },
            encoding: "utf-8",
            methods: "GET",
            uri: requestUrl
        }
        */
        request.get(requestUrl, (err, response, body) => {
            if (err) {
                console.log(err);
                resolve(null);
            }
            else {
                if (response.statusCode == 200) {
                    let xmlString = body;
                    let jsonObject = JSON.parse(xmlString);
                    let jobs = jsonObject['jobs'];
                    let job = jobs['job'];
                    resolve(job);
                    
                }
                else{
                    console.log('status code is not 200');
                    resolve(null);
                }

            }
        });

    });
}
function getPhotoUrl2(...args) {
    let posterId = args[0];
    return new Promise(function (resolve, reject) {
        let photoDetailUrl = 'http://www.saramin.co.kr/zf_user/jobs/relay/view-detail?rec_idx=' + posterId;
        request(photoDetailUrl, function (err, response, body) {
            if(err)
            {
                console.log(err);
                resolve(null);
            }
            const $ = cheerio.load(body);
            let imgSrc = null;
            $('img').each(function () {
                let imgTag = $(this);
                imgSrc = imgTag.attr('src');
            })
            if(imgSrc == null) {
                resolve(null);
            }
            else{
                resolve(imgSrc);
            }
        });
    });
}

function getImgSrc(...args) {
    let companyUrl = args[0];
    const defaultImgSrc = 'https://pds.saramin.co.kr/company/logo/201904/03/ppdcmy_oyl8-2rxicy_logo.png';
    if(companyUrl == null)
        return defaultImgSrc;
    return new Promise(function (resolve, reject) {
        request(companyUrl, function (err, response, body) {
            if(err)
            {
                console.log(err);
                resolve(defaultImgSrc);
            }
            const $ = cheerio.load(body);
            let imgSrc = null;
            $('.header_info > .title_info > .thumb_company > .inner_thumb > img').each(function () {
                let imgTag = $(this);
                imgSrc = imgTag.attr('src');
            })
            if(imgSrc)
                resolve(imgSrc);
            else
                resolve(defaultImgSrc);
        });
    });
}
function insertPosterByJob(...args) {
    let job = args[0];
    return new Promise(async function (resolve, reject) {
        let isValid = 0;

        for (var i = 0; i < job.length; i++) {
            var companyDetail = job[i]['company']['detail'];
            if (companyDetail == null)
                continue;
            let posterIdx;
            let categoryIdx = 4;
            let posterId = job[i]['id'];
            //디비에서 같은 아이디값 검색해서 이미 있으면 continue
            if (await alreadyReg(posterId))
                continue;

            console.log('posterId: ' + posterId);
            var photoUrl2 = await getPhotoUrl2(posterId);
            var companyUrl = companyDetail['href'];
            let companyName = companyDetail['name'];
            companyName = qs.escape(companyName);
            console.log(qs.unescape(companyName));
            var hostIdx = await getHostIdx(companyName);
            var posterStartDate = job[i]['posting-date'].substring(0, 10) + ' ' + job[i]['posting-date'].substring(11, 19);
            var posterEndDate = job[i]['expiration-date'].substring(0, 10) + ' ' + job[i]['expiration-date'].substring(11, 19);
            var publicOpenStartDate = posterStartDate;
            var publicOpenEndDate = posterEndDate;
            var documentDate = posterEndDate.substring(5, 16);
            var posterName = job[i]['position']['title'];
            var outline = job[i]['position']['industry']['name'];
            var jobCategoryText = job[i]['position']['job-category']['name'];
            var posterDetail = '사람인 웹사이트 참조';
            var posterWebsite = job[i]['url'];
            var interestStr = job[i]['position']['job-category']['code']; // 산업/업종 코드'1002,101,119,202,209', 803: 유통무역상사 -> 8: 특수계층·공공
            console.log('position: ' + JSON.stringify(job[i]['position']));
            let interestIdx = null;
            let keywordList = null;
            if(interestStr != null && jobCategoryText != null){
                interestIdx = interestStr.split(',');
                keywordList = jobCategoryText.split(',');
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
            }
            console.log('keyword complete');
            let target = '';
            target += job[i]['position']['experience-level']['name'];
            target += ', ' + job[i]['position']['required-education-level']['name'];
            let benefit = '';
            let locationCode = job[i]['position']['location']['code']; // 지역코드 '101010' 강남구

            benefit += '근무지역: ' + job[i]['position']['location']['name'];
            benefit += ', 연봉: ' + job[i]['salary']['name'];
            benefit = benefit.replace(/&gt;/gi, '>');
            let salaryCode = job[i]['salary']['code'];

            console.log('column complete');
            console.log('company url: ' + companyUrl);
            let imgSrc = await getImgSrc(companyUrl);
            console.log('imgSrc: ' + imgSrc);
            let now = new Date();
            let posterRegDate = moment(now).format("YYYY-MM-DD HH:mm:ss");
            let isOnlyUniv = 0;
            let contentIdx = 0;
            let adminAccept = 0;

            let params = [publicOpenStartDate, publicOpenEndDate, categoryIdx, imgSrc, photoUrl2, posterName, posterRegDate, posterStartDate, posterEndDate, posterWebsite, isOnlyUniv, outline, target, benefit, documentDate, contentIdx, hostIdx, posterDetail, adminAccept, keyword, posterId];

            connection.query('INSERT INTO poster(publicOpenStartDate, publicOpenEndDate, categoryIdx, photoUrl, photoUrl2, posterName, posterRegDate, posterStartDate, posterEndDate, posterWebsite, isOnlyUniv, outline, target, benefit, documentDate, contentIdx, hostIdx, posterDetail, adminAccept, keyword, posterId) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)', params, function (err, result) {
                if (err) {
                    console.log(err);
                }
                else {
                    console.log('poster 삽입성공');
                    posterIdx = result.insertId;
                    let alreadyInput = [];
                    if(interestIdx != null){
                        for (var j = 0; j < interestIdx.length; j++) {
                            interestIdx[j] = Math.floor(Number(interestIdx[j]) / 100) + 100;
                            if(alreadyInput.indexOf(interestIdx[j]) != -1)
                                continue;
                            alreadyInput.push(interestIdx[j]);
                            let params2 = [posterIdx, interestIdx[j]];
                            connection.query('INSERT INTO poster_interest(posterIdx, interestIdx) VALUES (?, ?)', params2, function (err2, result2) {
                                if (err2) {
                                    console.log(err2);
                                }
                                else {
                                    console.log('poster_interest 삽입성공');
                                }
                            });
                        }
                    }


                }
            });
        }
        isValid = 1;
        resolve(isValid);
    });
}

exports.handler = async (event, context, callback) => {
    const requestUrl = 'https://oapi.saramin.co.kr/job-search?access-key=xd5wcmyhxhpxPcAF1K5LAe46zBS48k6kqnYSGuHixO4ohdyWAXBDe&bbs_gb=0&job_type=4&edu_lv=&fields=posting-date+expiration-date+keyword-code+count&count=110';
    const job = await getJob(requestUrl);
    const isValid = await insertPosterByJob(job);
    console.log('is valid: ' + isValid);
}

