// db/compaction/compaction_predictor.h
#pragma once

#include <set>
#include <string>
#include <map>
#include "db/version_set.h"

namespace ROCKSDB_NAMESPACE {

class CompactionPredictor {
public:
  explicit CompactionPredictor(const VersionStorageInfo* vstorage) 
    : vstorage_(vstorage), predicted_files_() {}
  
  // 预测下一轮compaction会包含哪些文件
  std::set<std::string> PredictCompactionFiles();

  // 获取指定层级可能进行compaction的文件
  std::set<std::string> GetLevelCompactionFiles(int level);

  // 获取下一批可能进行compaction的文件，排除已经预测过的文件
  std::set<std::string> GetNextCompactionFilesFrom(int level, const std::set<std::string>& excluded_files);
  
  // 获取L0到L1 compaction可能涉及的L1文件
  std::set<std::string> GetPossibleTargetFilesForL0Compaction();
  
  // 获取与源层键范围重叠的目标层文件
  std::set<std::string> GetTargetLevelFilesForCompaction(
      int source_level, int target_level, const std::set<std::string>& source_files);

  // 检查该层级的score是否大于1
  bool CheckLevelScore(int level) {
    return vstorage_->CompactionScore(level) > 1.0;
  }

  // 计算当前层级在去掉一些文件后的新score
  double CalculateNewScore(int level, const std::set<std::string>& files_to_remove);

  // 检查键范围是否与指定文件重叠
  bool KeysInRangeOverlapWithFile(int level, 
                                const Slice& smallest_key,
                                const Slice& largest_key,
                                const std::string& file_number);

  // 检查两个键范围是否在指定层有顺序相关性
  bool Before(int level, 
             const Slice& smallest_key,
             const Slice& largest_key,
             const Slice& file_smallest, 
             const Slice& file_largest);

  // 检查中间层级的得分，如果中间层的得分都大于0.8，则返回true
  bool CheckIntermediateLevels(int upper_level, int lower_level) {
    return CheckIntermediateLevelsBetween(upper_level, lower_level);
  }

  // 检查中间层级的得分，如果中间层的得分都大于0.8，则返回true
  bool CheckIntermediateLevelsBetween(int start_level, int target_level);
  
  // 从预测集合中删除已经被compaction的文件
  void RemoveCompactedFiles(const std::set<std::string>& compacted_files);
  
  // 从预测集合中删除预测错误的文件
  void RemoveIncorrectPredictedFiles(const std::set<std::string>& incorrect_files);

private:
  const VersionStorageInfo* vstorage_;
  // 保存当前预测的文件集合及其出现次数
  std::map<std::string, int> predicted_files_;
};

} // namespace ROCKSDB_NAMESPACE