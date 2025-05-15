// db/compaction/compaction_predictor.h
#pragma once

#include <set>
#include <string>
#include <map>
#include "db/version_set.h"
#include <vector>

namespace ROCKSDB_NAMESPACE {

// 预测即将到来的压缩中包含的文件
class CompactionPredictor {
public:
  CompactionPredictor() : vstorage_(nullptr), immutable_options_(nullptr), mutable_cf_options_(nullptr) {}
  
  explicit CompactionPredictor(const VersionStorageInfo* vstorage) 
    : vstorage_(vstorage), predicted_files_(), immutable_options_(nullptr), mutable_cf_options_(nullptr) {}
  
  // 添加新的构造函数，接收options参数
  CompactionPredictor(const VersionStorageInfo* vstorage, 
                     const ImmutableOptions* immutable_options,
                     const MutableCFOptions* mutable_cf_options)
    : vstorage_(vstorage), predicted_files_(), 
      immutable_options_(immutable_options), 
      mutable_cf_options_(mutable_cf_options) {}
  
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
  
  // 检查是否可能发生L1到L2的compaction，尽管L1的score < 1.0
  bool CheckL1ToL2Compaction();

  // 获取预测的文件列表（用于兼容调用者）
  std::vector<uint64_t> GetPredictedFiles() const {
    std::vector<uint64_t> result;
    for (const auto& pair : predicted_files_) {
      // 尝试将文件名转换为数字
      try {
        uint64_t file_number = std::stoull(pair.first);
        result.push_back(file_number);
      } catch (const std::exception& e) {
        // 忽略无法转换的文件名
      }
    }
    return result;
  }

private:
  const VersionStorageInfo* vstorage_;
  // 保存当前预测的文件集合及其出现次数
  std::map<std::string, int> predicted_files_;
  // 添加options成员变量
  const ImmutableOptions* immutable_options_;
  const MutableCFOptions* mutable_cf_options_;
};

} // namespace ROCKSDB_NAMESPACE