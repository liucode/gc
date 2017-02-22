#define LENSIZE 4
#define BLOCKSIZE 4000
#define MEMRATE 0.8
#include "generator.h"
#include "time.h"
#include <algorithm>
#include <stdio.h>
#include<iostream>
#include <string>
#include <malloc.h>
#include <map>
#include <vector>
using namespace std;
typedef struct lognode
{
  string *key;
  int len;
  string *value;
}*lNode;

typedef struct hashnode
{
  string *key;
  bool valid;
  int offset;
}*hNode;

typedef pair<string, hNode> PAIR;  
  
bool cmp_by_value(const PAIR& lhs, const PAIR& rhs) {  
  return lhs.second->offset < rhs.second->offset;  
}  
  
struct CmpByValue {  
  bool operator()(const PAIR& lhs, const PAIR& rhs) {  
    return lhs.second->offset < rhs.second->offset;  
  }  
};  
class DB{
  public:
    const string notfound= "not found";
    FILE *fp;
    FILE *outfp;
    map<string, hNode> hashlist;
    map<string,string> memlist;
    
    int flag = -1;
    int total = 0;
    vector<int> blanknum;
    DB();
    void LiuHashWrite(string key,int offset,bool vaild);
    hNode LiuHashRead(string key);
    int Flush(FILE *flfp,lNode lnode,int offset);
    int LiuLogWrite(string *key,string *value);
    string LiuLogRead(string *key,int offset);
    void LiuWrite(string key,string value);
    string LiuRead(string key);
    void LiuDelete(string key);
    void PrintMap();
    vector<PAIR> SortMap();
    void Compact();
    void LiuCompact(int rate);
};


