import ballerina/io;
import ballerina/sql;
import ballerinax/mysql;

type R record {|
    string name;
    int age;
    "M"|"F" sex;
    string id;
|};

function arrayToR(string[] ar) returns R {
    string[] trimed = from string c in ar select c.trim();
    return {
        name: trimed[0],
        age: checkpanic int:fromString(trimed[1]),
        sex: extract(trimed[2]),
        id: trimed[3]
    };
}

function extract(string s) returns "M"|"F" {
    if (s.indexOf("m", 0) is int || s.indexOf("M", 0) is int) {
        return "M";
    }
    return "F";
}

public function main() returns error? {
    io:ReadableCSVChannel openReadableCsvFile = check io:openReadableCsvFile("path.csv");
    stream<string[], io:Error> csvStream = check openReadableCsvFile.csvStream();
    var next = csvStream.next();
    int count = 0;
    stream<R, error> s = from var row in csvStream
                         where row.length() > 3
                         select arrayToR(row);

    sql:ParameterizedQuery[] insertQueries = check from var r in s
                                            let sql:ParameterizedQuery q = `inset into _tab values (${r.id}, ${r.name}, ${r.sex}, ${r.age})`
                                            select q;
    mysql:Client 'client = check new("localhost", user = "root", password = "rootroot", port = 3306);
    sql:ExecutionResult[] batchExecute = check 'client->batchExecute(insertQueries);

    io:println("hi");
}
