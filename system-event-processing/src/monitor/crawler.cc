/*
 * crawler.cc
 *
 *  Created on: Mar 21, 2012
 *      Author: yxjiang
 */

#include "crawler.h"

namespace event
{

using namespace std;

Crawler::Crawler()
{
  boost::uuids::uuid uuid = boost::uuids::random_generator()();
  stringstream ss;
  ss << uuid;
  this->name_ = ss.str();
  pthread_rwlock_init(&rwlock_, NULL);
}

void Crawler::Init()
{
  this->SetCrawlerType();
  this->FetchStableMetaData();
  this->FetchMetaData();
}

Crawler::~Crawler()
{
  pthread_rwlock_destroy(&rwlock_);
}

string Crawler::GetCrawlerName() const
{
  return name_;
}

string Crawler::GetStreamType() const
{
  return type_;
}

std::map<std::string, std::string> Crawler::GetStableData() const
{
  return stableMetaData_; // no lock is needed
}

ObserveData Crawler::GetData()
{
  pthread_rwlock_rdlock(&rwlock_);
  ObserveData data = curData_;
  pthread_rwlock_unlock(&rwlock_);
  return data;
}

std::string CPUCrawler::statFile_ = "/proc/stat";

CPUCrawler::CPUCrawler(Mode mode)
    : Crawler(), mode_(mode)
{
}

CPUCrawler::~CPUCrawler()
{
}

/*
 * Fetch the CPU usage from the "/proc/stat" file.
 * To keep in high efficiency, we allocate memory from stack.
 */
void CPUCrawler::FetchMetaData()
{
  FILE *fp = fopen(statFile_.c_str(), "r");
  char cpu_id[8]; //	just for place order to hold the CPU ID
  int cpu_count = GetCPUCount();
//	int cpu_count = 16;
  if (mode_ == TOTAL_CPU)
  {
    unsigned long user_time, nice_time, sys_time, idle_time, iowait_time,
        int_time, softint_time;
    unsigned long unknown1_time, unknown2_time, unknown3_time;
    fscanf(fp, "%s %ld %ld %ld %ld %ld %ld %ld %ld %ld %ld\n", cpu_id,
        &user_time, &nice_time, &sys_time, &idle_time, &iowait_time, &int_time,
        &softint_time, &unknown1_time, &unknown2_time, &unknown3_time);

    int used_time_before = user_time + nice_time + sys_time;
    int total_time_before = used_time_before + idle_time;

    ThreadSleep(0, CRAWLER_SAMPLE_TIME_NANOSEC);
    rewind(fp);

    fscanf(fp, "%s %ld %ld %ld %ld %ld %ld %ld %ld %ld %ld\n", cpu_id,
        &user_time, &nice_time, &sys_time, &idle_time, &iowait_time, &int_time,
        &softint_time, &unknown1_time, &unknown2_time, &unknown3_time);

    int used_time_after = user_time + nice_time + sys_time;
    int total_time_after = used_time_before + idle_time;
    int used_period = used_time_after - used_time_before;
    int total_period = total_time_after - total_time_before;
    if (total_period < used_period)
      total_period = used_period;
    time(&(curData_.timestamp));

    boost::shared_ptr<std::map<string, string> > shared_map(
        new map<string, string>);
    stringstream ss;
    ss << static_cast<float>(used_period) / (total_period);
    shared_map->insert(make_pair<string, string>("cpu-usage-total", ss.str()));
    pthread_rwlock_wrlock(&rwlock_);
    curData_.properties_ = shared_map;
    pthread_rwlock_unlock(&rwlock_);
    //	cur_data.value = user_time;
  } else
  {
    unsigned long *user_time, *nice_time, *sys_time, *idle_time, *iowait_time,
        *int_time, *softint_time;
    unsigned long *unknown1_time, *unknown2_time, *unknown3_time;
    user_time = new unsigned long[cpu_count];
    nice_time = new unsigned long[cpu_count];
    sys_time = new unsigned long[cpu_count];
    idle_time = new unsigned long[cpu_count];
    iowait_time = new unsigned long[cpu_count];
    int_time = new unsigned long[cpu_count];
    softint_time = new unsigned long[cpu_count];
    unknown1_time = new unsigned long[cpu_count];
    unknown2_time = new unsigned long[cpu_count];
    unknown3_time = new unsigned long[cpu_count];

    fscanf(fp, "%s %ld %ld %ld %ld %ld %ld %ld %ld %ld %ld\n", cpu_id,
        &user_time[0], &nice_time[0], &sys_time[0], &idle_time[0],
        &iowait_time[0], &int_time[0], &softint_time[0], &unknown1_time[0],
        &unknown2_time[0], &unknown3_time[0]);
    for (int i = 0; i < cpu_count; ++i)
      fscanf(fp, "%s %ld %ld %ld %ld %ld %ld %ld %ld %ld %ld\n", cpu_id,
          &user_time[i], &nice_time[i], &sys_time[i], &idle_time[i],
          &iowait_time[i], &int_time[i], &softint_time[i], &unknown1_time[i],
          &unknown2_time[i], &unknown3_time[i]);

    int *used_time_before = new int[cpu_count];
    int *total_time_before = new int[cpu_count];

    for (int i = 0; i < cpu_count; ++i)
    {
      used_time_before[i] = user_time[i] + nice_time[i] + sys_time[i];
      total_time_before[i] = used_time_before[i] + idle_time[i];
    }

    ThreadSleep(0, CRAWLER_SAMPLE_TIME_NANOSEC);
    rewind(fp);
    fscanf(fp, "%s %ld %ld %ld %ld %ld %ld %ld %ld %ld %ld\n", cpu_id,
        &user_time[0], &nice_time[0], &sys_time[0], &idle_time[0],
        &iowait_time[0], &int_time[0], &softint_time[0], &unknown1_time[0],
        &unknown2_time[0], &unknown3_time[0]);
    for (int i = 0; i < cpu_count; ++i)
      fscanf(fp, "%s %ld %ld %ld %ld %ld %ld %ld %ld %ld %ld\n", cpu_id,
          &user_time[i], &nice_time[i], &sys_time[i], &idle_time[i],
          &iowait_time[i], &int_time[i], &softint_time[i], &unknown1_time[i],
          &unknown2_time[i], &unknown3_time[i]);

    time(&(curData_.timestamp));
    boost::shared_ptr<std::map<string, string> > shared_map(
        new map<string, string>);
    int *used_time_after = new int[cpu_count];
    int *total_time_after = new int[cpu_count];
    int *used_period = new int[cpu_count];
    int *total_period = new int[cpu_count];
    for (int i = 0; i < cpu_count; ++i)
    {
      used_time_after[i] = user_time[i] + nice_time[i] + sys_time[i];
      total_time_after[i] = used_time_before[i] + idle_time[i];
      used_period[i] = used_time_after[i] - used_time_before[i];
      total_period[i] = total_time_after[i] - total_time_before[i];
      if (total_period[i] < used_period[i])
        total_period[i] = used_period[i];
      stringstream ss;
      ss << "cpu-usage-" << i;
      string prop_name;
      prop_name = ss.str();
      ss.str("");
      float prop = 0;
      if (total_period[i] != 0.0)
      {
        prop = static_cast<float>(used_period[i]) / (total_period[i]);
      }
      ss << prop; //	check if prop is NaN
      shared_map->insert(make_pair<string, string>(prop_name, ss.str()));
    }

    pthread_rwlock_wrlock(&rwlock_);
    curData_.properties_ = shared_map;
    pthread_rwlock_unlock(&rwlock_);

    //	release
    delete user_time;
    delete nice_time;
    delete sys_time;
    delete idle_time;
    delete iowait_time;
    delete int_time;
    delete softint_time;
    delete unknown1_time;
    delete unknown2_time;
    delete unknown3_time;
    delete used_time_before;
    delete total_time_before;
    delete used_time_after;
    delete total_time_after;
    delete used_period;
    delete total_period;
  }
  fclose(fp);
//  ThreadSleep(0, CRAWLER_SLEEP_TIME_NANOSEC);
}

void CPUCrawler::SetCrawlerType()
{
  this->type_ = "CPU-Utilization";
}

void CPUCrawler::FetchStableMetaData()
{
  int numOfCores = GetCPUCount();
  char numOfCoresStr[8];
  sprintf(numOfCoresStr, "%d", numOfCores);
  stableMetaData_.insert(
      make_pair<string, string>("numberOfCores", numOfCoresStr));

  ifstream fin("/proc/cpuinfo");
  string line;
  getline(fin, line); //  skip first line
  getline(fin, line);
  vector<string> vendorIDKeyValue;
  Split(line, ':', vendorIDKeyValue, true);
  stableMetaData_.insert(
      make_pair<string, string>("vendorName", vendorIDKeyValue[1]));
  getline(fin, line);
  getline(fin, line);
  getline(fin, line);
  vector<string> modelNameKeyValue;
  Split(line, ':', modelNameKeyValue, true);
  stableMetaData_.insert(
      make_pair<string, string>("vendorName", modelNameKeyValue[1]));
  getline(fin, line);
  getline(fin, line);
  getline(fin, line);
  vector<string> CPUMHz;
  Split(line, ':', CPUMHz, true);
  stableMetaData_.insert(make_pair<string, string>("CPUMHz", CPUMHz[1]));

  fin.close();
}

//string MemCrawler::memStatFile_ = "/proc/meminfo";
///*
// * Create a new crawler to crawl the memory usage of the target system.
// */
//MemCrawler::MemCrawler() : Crawler()
//{
//}
//
//MemCrawler::~MemCrawler()
//{
//}
//
///*
// * Get the general memory usage
// */
//void MemCrawler::FetchMetaData()
//{
//  FILE *fp = fopen(memStatFile_.c_str(), "r");
//
//  time(&(curData_.timestamp));
//  boost::shared_ptr< std::map< string, string> > shared_map(new map<string, string>);
//  char buf[128];
//  char value[16];
//  char buf_kb[4];
//  for (int i = 0; i < 6; ++i)
//  {
//    fscanf(fp, "%s %s %s\n", buf, value, buf_kb);
//    shared_map->insert(make_pair<string, string>(buf, value));
//  }
//  pthread_rwlock_wrlock(&rwlock_);
//  curData_.properties_ = shared_map;
//  pthread_rwlock_unlock(&rwlock_);
//  fclose(fp);
//  ThreadSleep(0, CRAWLER_SLEEP_TIME_NANOSEC);
//}
//
//string NetCrawler::netStatFile_ = "/proc/net/dev";
//
//NetCrawler::NetCrawler() : Crawler()
//{
//}
//
//NetCrawler::~NetCrawler()
//{
//}
//
///*
// * Get the general network usage.
// */
//void NetCrawler::FetchMetaData()
//{
//  FILE *fp = fopen(netStatFile_.c_str(), "r");
//  char label[16];	//	labels lick lo: eth0: ppp0 etc.
//  long in_bytes[2], in_packets[2], in_errs[2], in_drop[2], in_fifo[2],
//      in_frame[2], in_compressed[2], in_multicast[2];
//  long out_bytes[2], out_packets[2], out_errs[2], out_drop[2], out_fifo[2],
//      out_colls[2], out_carrier[2], out_compressed[2];
//
//  char buf[256];
//  fgets(buf, 256, fp);	//	skip first line
//  fgets(buf, 256, fp);	//	skip second line
//  fgets(buf, 256, fp);	//	skip third line
//  fgets(buf, 256, fp);
//  sscanf(buf, "%s %ld %ld %ld %ld %ld %ld %ld %ld %ld %ld %ld %ld %ld %ld %ld %ld\n",
//      label, &in_bytes[0], &in_packets[0], &in_errs[0], &in_drop[0],
//      &in_fifo[0], &in_frame[0], &in_compressed[0], &in_multicast[0],
//      &out_bytes[0], &out_packets[0], &out_errs[0], &out_drop[0], &out_fifo[0],
//      &out_colls[0], &out_carrier[0], &out_compressed[0]);
//
//  ThreadSleep(0, CRAWLER_SAMPLE_TIME_NANOSEC);
//
//  rewind(fp);
//  fgets(buf, 256, fp);	//	skip first line
//  fgets(buf, 256, fp);	//	skip second line
//  fgets(buf, 256, fp);	//	skip third line
//  fgets(buf, 256, fp);
//  sscanf(buf, "%s %ld %ld %ld %ld %ld %ld %ld %ld %ld %ld %ld %ld %ld %ld %ld %ld\n",
//      label, &in_bytes[1], &in_packets[1], &in_errs[1], &in_drop[1],
//      &in_fifo[1], &in_frame[1], &in_compressed[1], &in_multicast[1],
//      &out_bytes[1], &out_packets[1], &out_errs[1], &out_drop[1], &out_fifo[1],
//      &out_colls[1], &out_carrier[1], &out_compressed[1]);
//  fclose(fp);
//
//  int multiply = 1000000000 / CRAWLER_SAMPLE_TIME_NANOSEC;
////	int multiply = 2;
//  float recv_speed = static_cast<float>(in_bytes[1] - in_bytes[0]) / 1024
//      * multiply;
//  float send_speed = static_cast<float>(out_bytes[1] - out_bytes[0]) / 1024
//      * multiply;
//  stringstream ss;
//  boost::shared_ptr< std::map< string, string> > shared_map(new map<string, string>);
//  ss << recv_speed;
//  shared_map->insert(make_pair< string, string >("receive speed (KB):", ss.str()));
//  ss.str("");
//  ss << send_speed;
//  shared_map->insert(make_pair< string, string >("send speed (KB):", ss.str()));
//  pthread_rwlock_wrlock(&rwlock_);
//  curData_.properties_ = shared_map;
//  pthread_rwlock_unlock(&rwlock_);
//
//  ThreadSleep(0, CRAWLER_SLEEP_TIME_NANOSEC);
//}
//
//string DiskCrawler::diskStatPipe_ = "df -m";
//
//DiskCrawler::DiskCrawler() : Crawler()
//{
//}
//
//DiskCrawler::~DiskCrawler()
//{
//}
//
//void DiskCrawler::FetchMetaData()
//{
//  FILE *fp;
//  if ((fp = popen(diskStatPipe_.c_str(), "r")) == NULL)
//  {
//    fprintf(stderr, "Open disk stat pipe failed.\n");
//    return;
//  }
//
//  boost::shared_ptr<std::map<string, string> > shared_map(
//      new map<string, string>);
//
//  char name[256];
//  char total[16];
//  char used[16];
//  char avail[16];
//  char prop[4];
//  char mount[256];
//  fgets(name, sizeof(name), fp);	//	skip first line
//  while (!feof(fp))
//  {
//    fscanf(fp, "%s %s %s %s %s %s\n", name, total, used, avail, prop, mount);
//    char disk_name[256];
//    bzero(disk_name, strlen(disk_name));
//    strcpy(disk_name, name);
//    strcat(disk_name, "=");
//    shared_map->insert(
//        make_pair<string, string>(strcat(disk_name, "disk-name:"), name));
//    bzero(disk_name, strlen(disk_name));
//    strcpy(disk_name, name);
//    strcat(disk_name, "=");
//    shared_map->insert(
//        make_pair<string, string>(strcat(disk_name, "total-size (MB):"),
//            total));
//    bzero(disk_name, strlen(disk_name));
//    strcpy(disk_name, name);
//    strcat(disk_name, "=");
//    shared_map->insert(make_pair<string, string>(strcat(disk_name, "used-size (MB):"), used));
//    bzero(disk_name, strlen(disk_name));
//    strcpy(disk_name, name);
//    strcat(disk_name, "=");
//    shared_map->insert(make_pair<string, string>(strcat(disk_name, "available-size (MB):"), avail));
//    bzero(disk_name, strlen(disk_name));
//    strcpy(disk_name, name);
//    strcat(disk_name, "=");
//    shared_map->insert(
//        make_pair<string, string>(strcat(disk_name, "used-proportion:"), prop));
//    bzero(disk_name, strlen(disk_name));
//    strcpy(disk_name, name);
//    strcat(disk_name, "=");
//    shared_map->insert(
//        make_pair<string, string>(strcat(disk_name, "mounted-on:"), mount));
//  }
//  fclose(fp);
//  pthread_rwlock_wrlock(&rwlock_);
//  curData_.properties_ = shared_map;
//  pthread_rwlock_unlock(&rwlock_);
//
//  ThreadSleep(0, CRAWLER_SLEEP_TIME_NANOSEC);
//}

};
