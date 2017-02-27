#define WIN32_LEAN_AND_MEAN
#include<Windows.h>
#include<cstdio>
#include<io.h>
#include<string>
#include<vector>
#include<iostream>
#include<fstream>
#include<sstream>
#include<vector>
#include<ctime>
#include"mongoc.h"
#include"bson.h"
mongoc_client_t *client;
mongoc_collection_t *collection;

void SplitString(const std::string& s, std::vector<std::string>& v, const std::string& c)
{
	std::string::size_type pos1, pos2;
	pos2 = s.find(c);
	pos1 = 0;
	while (std::string::npos != pos2)
	{
		v.push_back(s.substr(pos1, pos2 - pos1));

		pos1 = pos2 + c.size();
		pos2 = s.find(c, pos1);
	}
	if (pos1 != s.length())
		v.push_back(s.substr(pos1));
}

time_t convert_string_to_time_t(const std::string & time_string)
{
	struct tm tm1;
	time_t time1;
	int i = sscanf(time_string.c_str(), "%d/%d/%d %d:%d",
		&(tm1.tm_year),
		&(tm1.tm_mon),
		&(tm1.tm_mday),
		&(tm1.tm_hour),
		&(tm1.tm_min));
	tm1.tm_sec = 0;
	tm1.tm_year -= 1900;
	tm1.tm_mon--;
	tm1.tm_isdst = -1;
	time1 = mktime(&tm1);
	return time1*1000;
}

std::string getDate(int unixdatetime)
{
	const time_t tt = unixdatetime;
	struct tm* ptm = localtime(&tt);
	char date[60] = { 0 };
	sprintf(date, "%d-%02d-%02d ", (int)ptm->tm_year + 1900, (int)ptm->tm_mon + 1, (int)ptm->tm_mday);
	return std::string(date);
}

std::string getTime(int unixdatetime)
{
	const time_t tt = unixdatetime;
	struct tm* ptm = localtime(&tt);
	char time[60] = { 0 };
	sprintf(time, "%02d:%02d:%02d", (int)ptm->tm_hour , (int)ptm->tm_min, (int)ptm->tm_sec);
	return std::string(time);
}

void insert_to_mongodb(std::string line,std::string symbol)
{
	//时间 开 高 底 收 量 持仓
	time_t mongodatetime;
	double open;
	double high;
	double low;
	double close;
	double volume;
	double openInterest;

	///////////////////////
	std::string temp;
	
	std::istringstream is(line);
	int i = 0;
	while (std::getline(is, temp, ','))
	{
		if (i == 0)
		{
			mongodatetime = convert_string_to_time_t(temp);
		}
		else if (i == 1)
		{
			open = atof(temp.c_str());
		}
		else if (i == 2)
		{
			high = atof(temp.c_str());
		}
		else if (i == 3)
		{
			low = atof(temp.c_str());

		}
		else if (i == 4)
		{
			close = atof(temp.c_str());

		}
		else if (i == 5)
		{
			volume = atof(temp.c_str());
		}
		else if (i == 6)
		{
			openInterest = atof(temp.c_str());
		}
		i++;
	}

	if (temp == "")//剔除空的
	{
		return;
	}
	time_t unixdatetime = mongodatetime/1000;
	bson_error_t error;
	bson_oid_t oid;
	bson_t *doc;
	doc = bson_new();
	bson_oid_init(&oid, NULL);
	BSON_APPEND_OID(doc, "_id", &oid);
	BSON_APPEND_UTF8(doc, "exchange", "");
	BSON_APPEND_UTF8(doc, "symbol", symbol.c_str());

	BSON_APPEND_DOUBLE(doc, "open", open);
	BSON_APPEND_DOUBLE(doc, "high", high);
	BSON_APPEND_DOUBLE(doc, "low", low);
	BSON_APPEND_DOUBLE(doc, "close", close);
	BSON_APPEND_DOUBLE(doc, "volume", volume);

	BSON_APPEND_DATE_TIME(doc, "datetime", mongodatetime);
	BSON_APPEND_UTF8(doc, "date", getDate(unixdatetime).c_str());
	BSON_APPEND_UTF8(doc, "time", getTime(unixdatetime).c_str());
	BSON_APPEND_DOUBLE(doc, "openInterest", openInterest);

	BSON_APPEND_DOUBLE(doc, "openPrice", 0);
	BSON_APPEND_DOUBLE(doc, "highPrice", 0);
	BSON_APPEND_DOUBLE(doc, "lowPrice", 0);
	BSON_APPEND_DOUBLE(doc, "preClosePrice", 0);

	BSON_APPEND_DOUBLE(doc, "upperLimit", 0);
	BSON_APPEND_DOUBLE(doc, "lowerLimit", 0);

	// 将bson文档插入到集合
	if (!mongoc_collection_insert(collection, MONGOC_INSERT_NONE, doc, NULL, &error))
	{
		std::cout << "插入失败"<<std::endl;
	}
	bson_destroy(doc);
}

int main()
{
	//mongo
	mongoc_init();
	std::vector<std::string> name;
	struct _finddata_t files;
	int File_Handle;
	int i = 0;
	File_Handle = _findfirst("./*.csv", &files);
	if (File_Handle == -1)
	{
		printf("error\n");
		system("pause");
		return 0;
	}
	do
	{
		name.push_back(files.name);
		i++;
	} 
	while (0 == _findnext(File_Handle, &files));
	_findclose(File_Handle);
	//获取目录中所有文件
	for (std::vector<std::string>::iterator it = name.begin(); it != name.end(); it++)
	{
		std::vector<std::string>t;
		SplitString((*it), t, ".");
		bson_error_t error;
		int64_t count;
		client = mongoc_client_new("mongodb://localhost:27017/");
		collection = mongoc_client_get_collection(client, "CTPMinuteDb", t[0].c_str());
		std::string filename = "./" + (*it);
		std::ifstream  f(filename);
		if (!f.is_open())
		{
			std::cout << "不能打开文件" << filename << std::endl;
			break;
		}
		//打开CSV文件开始读取
		std::cout << "开始读取文件" << *it << std::endl;
		std::string line;
		while (f.good())
		{
			std::getline(f, line);
			insert_to_mongodb(line,t[0]);
		}
		mongoc_collection_destroy(collection);
		mongoc_client_destroy(client);
	}
	system("pause");
	mongoc_cleanup();
	return 0;
}