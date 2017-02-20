#include "file.h"
#define VSIZE 512
DB::DB()
{
  fp = fopen("fscant","a+");
  if(fp == NULL)
  {
    printf("the file is opeaned error!\n");
  }
}

void DB::LiuHashWrite(string key,int offset,bool valid)
{
    hNode hnode = (hNode)malloc(sizeof(hashnode));
    hnode->key = &key;
    hnode->valid = valid;
    hnode->offset = offset;
    hashlist[key] = hnode;
    return;
}

hNode DB::LiuHashRead(string key)
{
   map<string, hNode> ::iterator it = hashlist.find(key);
  if(it!=hashlist.end())
    return hashlist[key]; 
  else
    return NULL;
}


int DB::Flush(FILE *flfp,lNode lnode,int offset)
{
  fseek(flfp,offset,SEEK_SET);
  int diff = BLOCKSIZE-offset%BLOCKSIZE;
  int total = lnode->key->length()+LENSIZE+lnode->len;
  //设置死了
  if(total>diff)//补0操作
  {
      char c = 't';
      fwrite(&c,sizeof(char),diff,flfp);
      //结尾剩
      if(blanknum.size()<=(offset/BLOCKSIZE))
      {
            blanknum.push_back(diff);
      }
      offset += diff;
  }
  else if(diff == total)
  {
      blanknum.push_back(0);
  }
  else
  {
  }
  if(lnode->len!=0)
    fprintf(flfp,"%s%04d%s",lnode->key->c_str(),lnode->len,lnode->value->c_str());
  else
    fprintf(flfp,"%s%04d",lnode->key->c_str(),lnode->len);
  return offset;
}
int  DB::LiuLogWrite(string *key,string *value)
{
  lNode lnode = (lNode)malloc(sizeof(lognode));
  lnode->key = key;
  lnode->len = value->length();
  lnode->value = value;
  fseek(fp,0,SEEK_END);//到末尾
  int offset = ftell(fp);
  return Flush(fp,lnode,offset);
}

string DB::LiuLogRead(string *key,int offset)
{
  if(fseek(fp,offset,0)!=0)
  {
    printf("fseek error");
    exit(0);
  }
  
  char fkey[key->length()];
  fread(&fkey,sizeof(char)*key->length(),1,fp);
  string temp(fkey,key->length());
  
  if(temp.compare(*key)!=0)
  {
    printf("key error");
    exit(0);
  }
  
  char slen[LENSIZE];
  fread(slen,sizeof(int),1,fp);
  int len = atoi(slen);
  
  char fvalue[len];
  fread(&fvalue,sizeof(char)*len,1,fp);
  string s(fvalue,len);
  return s;
}

void DB::LiuWrite(string key,string value)
{
    hNode hnode = LiuHashRead(key);
    if(hnode!=NULL)
    {
      string prev = LiuLogRead(&key,hnode->offset);
      blanknum[(hnode->offset)/BLOCKSIZE]+=(key.length()+LENSIZE+prev.length());
    }

  int offset = LiuLogWrite(&key,&value);
  LiuHashWrite(key,offset,true);
}

string DB::LiuRead(string key)
{
  map<string, string> ::iterator it = memlist.find(key);
  if(it!=memlist.end())
      return it->second;
  else
  {      
   hNode hnode = LiuHashRead(key);
   if(hnode==NULL||!hnode->valid)
     return notfound; 
   return LiuLogRead(&key,hnode->offset);
  }
}

void DB::LiuDelete(string key)
{
    hNode hnode = LiuHashRead(key);
    if(hnode!=NULL)
    {
      string prev = LiuLogRead(&key,hnode->offset);
      blanknum[(hnode->offset)/BLOCKSIZE]+=(key.length()+LENSIZE+prev.length());
    }
    string value = "";
    int offset = LiuLogWrite(&key,&value);
    LiuHashWrite(key,offset,false);
}

vector<PAIR> DB::SortMap()
{
    vector<PAIR> name_score_vec(hashlist.begin(), hashlist.end()); 
    sort(name_score_vec.begin(), name_score_vec.end(), CmpByValue());
    return name_score_vec;
}
void DB::PrintMap()
{
  vector<PAIR> name_score_vec = SortMap(); 
  for (int i = 0; i != name_score_vec.size(); ++i) {  
    cout << name_score_vec[i].first << endl;  
  } 
}


void DB::Compact()
{
    vector<PAIR> cmap = SortMap();
    outfp = fopen("comp","a+");
    for (int i = 0; i != cmap.size(); ++i) {
       hNode hnode = LiuHashRead(cmap[i].first);
       if(hnode != NULL&&hnode->valid)
       {
          string value = LiuLogRead(&(cmap[i].first),cmap[i].second->offset);
          lNode lnode = (lNode)malloc(sizeof(lognode));
          lnode->key = &(cmap[i].first);
          lnode->len = value.length();
          lnode->value = &value;
          int newoffset = ftell(outfp);
          int reoffset = Flush(outfp,lnode,newoffset);
          LiuHashWrite(cmap[i].first,reoffset,true);
       }
       else
        {
           map<string, hNode> ::iterator it = hashlist.find(cmap[i].first);
           if(it!=hashlist.end())
              hashlist.erase(it);
        }
    }
    fclose(fp);
    fp = outfp;
}
void DB::LiuCompact()
{
    outfp = fopen("comp","a+");
    vector<PAIR> cmap = SortMap();
    for (int i = 0; i != cmap.size(); ++i) 
    {
      hNode hnode = LiuHashRead(cmap[i].first);
      if(hnode != NULL&&hnode->valid)
      {
        if(blanknum[cmap[i].second->offset/BLOCKSIZE]<=(BLOCKSIZE/2))//空少copy
        {
          string value = LiuLogRead(&(cmap[i].first),cmap[i].second->offset);
          //memlist[cmap[i].first] = value;
          if(flag!=cmap[i].second->offset/BLOCKSIZE)
          {
              total += blanknum[cmap[i].second->offset/BLOCKSIZE];
              flag = cmap[i].second->offset/BLOCKSIZE;
          }
        }
        else
        {
          string value = LiuLogRead(&(cmap[i].first),cmap[i].second->offset);
          lNode lnode = (lNode)malloc(sizeof(lognode));
          lnode->key = &(cmap[i].first);
          lnode->len = value.length();
          lnode->value = &value;
          int newoffset = ftell(outfp);
          int reoffset = Flush(outfp,lnode,newoffset);
          LiuHashWrite(cmap[i].first,reoffset,true);
       }//if bl
      }//if hnode
      else
      {
           map<string, hNode> ::iterator it = hashlist.find(cmap[i].first);
           if(it!=hashlist.end())
              hashlist.erase(it);
      }//if hnode
    }//for
    fclose(fp);
    fp = outfp;
}


void test(FILE *dfp)
{
  int i;
  clock_t starts,ends;
  DB* db = new DB();
  for(i=0;i<=2000000;i++)
  {
   char v[VSIZE];
   for(int j=0;j<VSIZE;j++)
      v[j] = '0';
   string value = v; 
   string key = to_string(i);  
   db->LiuWrite(key,value);
  }
  fflush(db->fp);

  while(!feof(dfp))
  {
    char key[7];
    fscanf(dfp,"%s",key);
    string s = key;
    db->LiuDelete(s);
  }
  starts = clock();
  db->LiuCompact();
  fflush(db->fp);
  ends = clock();
  printf("time: %d %f\n",ends-starts,db->total);
}
int main(int argc,char** argv)
{
  FILE *dfp = fopen("mix","r");
  test(dfp);
  fclose(dfp);
  return 0;
}

