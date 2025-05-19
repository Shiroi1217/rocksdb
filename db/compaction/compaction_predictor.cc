// db/compaction/compaction_predictor.cc
#include "db/compaction/compaction_predictor.h"
#include "util/string_util.h"
#include "db/version_set.h"
#include "logging/logging.h"
#include <sstream>

namespace ROCKSDB_NAMESPACE {

// 由于构造函数在头文件中已定义为内联函数，这里删除构造函数定义
// CompactionPredictor::CompactionPredictor(const VersionStorageInfo* vstorage)
//     : vstorage_(vstorage) {}

// 辅助函数：将Slice转为可读字符串（优先ASCII，否则16进制）
static std::string ToReadableString(const Slice& s) {
  std::string str = s.ToString(false); // false: 尽量输出可打印字符
  // 检查是否全为可打印字符，否则用16进制
  for (char c : str) {
    if (c < 32 || c > 126) {
      return s.ToString(true); // true: 强制16进制
    }
  }
  return str;
}

std::set<std::string> CompactionPredictor::PredictCompactionFiles() {
  if (info_log_ != nullptr) {
    ROCKS_LOG_INFO(info_log_, "PredictCompactionFiles: 正在获取最新的层信息和分数值");
  }
  std::vector<int> levels_to_check;
  for (int level = 0; level < vstorage_->num_levels() - 1; level++) {
    if (vstorage_->CompactionScore(level) > 1.0) {
      levels_to_check.push_back(level);
      if (info_log_ != nullptr) {
        ROCKS_LOG_INFO(info_log_, "层级 %d 的分数为 %.2f > 1.0，将进行预测", 
                       level, vstorage_->CompactionScore(level));
      }
    } else {
      for (int upper = 0; upper < level; ++upper) {
        if (vstorage_->CompactionScore(upper) > 1.0) {
          bool all_above_08 = true;
          for (int l = upper + 1; l <= level; ++l) {
            if (vstorage_->CompactionScore(l) <= 0.8) {
              all_above_08 = false;
              break;
            }
          }
          if (all_above_08) {
            levels_to_check.push_back(level);
            if (info_log_ != nullptr) {
              ROCKS_LOG_INFO(info_log_, "层级 %d 的分数为 %.2f <= 1.0，但上层 %d 分数 > 1.0 且[%d,%d]所有层分数都 > 0.8，将进行预测", 
                             level, vstorage_->CompactionScore(level), upper, upper+1, level);
            }
            break;
          }
        }
      }
    }
  }
  std::set<std::string> result;
  std::set<std::string> predicted_files; // 新增：全局已预测SST文件集合
  if (levels_to_check.empty()) {
    if (info_log_ != nullptr) {
      ROCKS_LOG_INFO(info_log_, "没有层级需要进行compaction预测");
    }
    return result;
  }
  // L0->L1特殊处理
  if (vstorage_->CompactionScore(0) > 1.0) {
    if (info_log_ != nullptr) {
      ROCKS_LOG_INFO(info_log_, "L0的分数为 %.2f > 1.0，开始预测L0到L1的compaction", 
                     vstorage_->CompactionScore(0));
    }
    std::set<std::string> l1_files = GetPossibleTargetFilesForL0Compaction();
    if (!l1_files.empty()) {
      if (info_log_ != nullptr) {
        ROCKS_LOG_INFO(info_log_, "为L0到L1 compaction预测了 %zu 个L1文件", l1_files.size());
        std::string files_str;
        for (const auto& file : l1_files) {
          files_str += file + " ";
        }
        ROCKS_LOG_DEBUG(info_log_, "预测的L1文件: %s", files_str.c_str());
      }
      result.insert(l1_files.begin(), l1_files.end());
      predicted_files.insert(l1_files.begin(), l1_files.end());
    } else {
      if (info_log_ != nullptr) {
        ROCKS_LOG_INFO(info_log_, "没有找到L0到L1 compaction的目标文件");
      }
    }
  }
  // 只对1层及以上做"排除已被预测SST文件"逻辑
  for (int level : levels_to_check) {
    if (level == 0 && vstorage_->CompactionScore(0) > 1.0) {
      continue;
    }
    if (info_log_ != nullptr) {
      ROCKS_LOG_INFO(info_log_, "开始预测层级 %d 的compaction，当前分数: %.2f", 
                     level, vstorage_->CompactionScore(level));
    }
    std::set<std::string> level_files = GetLevelCompactionFiles(level);
    // 新增：排除已被低层预测的SST文件
    std::set<std::string> filtered_files;
    for (const auto& file : level_files) {
      if (predicted_files.count(file) == 0) {
        filtered_files.insert(file);
      }
    }
    if (!filtered_files.empty()) {
      result.insert(filtered_files.begin(), filtered_files.end());
      predicted_files.insert(filtered_files.begin(), filtered_files.end());
    }
    // 预测下一层的文件
    if (level + 1 < vstorage_->num_levels()) {
      if (info_log_ != nullptr) {
        ROCKS_LOG_INFO(info_log_, "预测层级 %d 到 %d 的compaction涉及的目标层文件", 
                       level, level + 1);
      }
      std::set<std::string> target_files = GetTargetLevelFilesForCompaction(
          level, level + 1, filtered_files);
      if (!target_files.empty()) {
        if (info_log_ != nullptr) {
          ROCKS_LOG_INFO(info_log_, "为层级 %d 到 %d 的compaction预测了 %zu 个目标层文件", 
                         level, level + 1, target_files.size());
          std::string files_str;
          for (const auto& file : target_files) {
            files_str += file + " ";
          }
          ROCKS_LOG_DEBUG(info_log_, "预测的层级 %d 文件: %s", level + 1, files_str.c_str());
        }
        // 目标层文件也要排除已被预测的
        std::set<std::string> filtered_target_files;
        for (const auto& file : target_files) {
          if (predicted_files.count(file) == 0) {
            filtered_target_files.insert(file);
          }
        }
        if (!filtered_target_files.empty()) {
          result.insert(filtered_target_files.begin(), filtered_target_files.end());
          predicted_files.insert(filtered_target_files.begin(), filtered_target_files.end());
        }
      } else {
        if (info_log_ != nullptr) {
          ROCKS_LOG_INFO(info_log_, "没有找到层级 %d 到 %d 的compaction目标文件", 
                         level, level + 1);
        }
      }
    }
    // 计算新的score、尝试多轮预测等，均用filtered_files和predicted_files
    double new_score = CalculateNewScore(level, filtered_files);
    if (info_log_ != nullptr) {
      ROCKS_LOG_INFO(info_log_, "层级 %d 在预测compaction后的新分数: %.2f", 
                     level, new_score);
    }
    int attempt = 1;
    std::set<std::string> excluded_files = filtered_files;
    while (new_score > 1.0 && attempt < 3) {
      if (info_log_ != nullptr) {
        ROCKS_LOG_INFO(info_log_, "层级 %d 的新分数 %.2f 仍然 > 1.0，尝试预测更多文件（尝试 %d/3）", 
                       level, new_score, attempt + 1);
      }
      std::set<std::string> additional_files = GetNextCompactionFilesFrom(level, excluded_files);
      // 新增：排除已被预测的
      std::set<std::string> filtered_additional_files;
      for (const auto& file : additional_files) {
        if (predicted_files.count(file) == 0) {
          filtered_additional_files.insert(file);
        }
      }
      if (filtered_additional_files.empty()) {
        if (info_log_ != nullptr) {
          ROCKS_LOG_INFO(info_log_, "没有找到层级 %d 的更多文件", level);
        }
        break;
      }
      if (info_log_ != nullptr) {
        ROCKS_LOG_INFO(info_log_, "为层级 %d 额外预测了 %zu 个文件", 
                       level, filtered_additional_files.size());
        std::string files_str;
        for (const auto& file : filtered_additional_files) {
          files_str += file + " ";
        }
        ROCKS_LOG_DEBUG(info_log_, "额外预测的文件: %s", files_str.c_str());
      }
      result.insert(filtered_additional_files.begin(), filtered_additional_files.end());
      predicted_files.insert(filtered_additional_files.begin(), filtered_additional_files.end());
      excluded_files.insert(filtered_additional_files.begin(), filtered_additional_files.end());
      // 预测下一层的文件
      if (level + 1 < vstorage_->num_levels()) {
        std::set<std::string> additional_target_files = GetTargetLevelFilesForCompaction(
            level, level + 1, filtered_additional_files);
        // 目标层文件也要排除已被预测的
        std::set<std::string> filtered_additional_target_files;
        for (const auto& file : additional_target_files) {
          if (predicted_files.count(file) == 0) {
            filtered_additional_target_files.insert(file);
          }
        }
        if (!filtered_additional_target_files.empty()) {
          result.insert(filtered_additional_target_files.begin(), filtered_additional_target_files.end());
          predicted_files.insert(filtered_additional_target_files.begin(), filtered_additional_target_files.end());
        }
      }
      new_score = CalculateNewScore(level, excluded_files);
      if (info_log_ != nullptr) {
        ROCKS_LOG_INFO(info_log_, "层级 %d 在额外预测compaction后的新分数: %.2f", 
                       level, new_score);
      }
      attempt++;
    }
  }
  for (const auto& file : result) {
    predicted_files_[file]++;
  }
  if (info_log_ != nullptr) {
    if (!result.empty()) {
      ROCKS_LOG_INFO(info_log_, "总共预测了 %zu 个文件用于下一轮compaction", result.size());
      std::string files_str;
      for (const auto& file : result) {
        files_str += file + " ";
      }
      ROCKS_LOG_DEBUG(info_log_, "所有预测的文件: %s", files_str.c_str());
    } else {
      ROCKS_LOG_INFO(info_log_, "没有预测到任何文件用于下一轮compaction");
    }
  }
  return result;
}

// 获取L0到L1 compaction可能涉及的L1文件
std::set<std::string> CompactionPredictor::GetPossibleTargetFilesForL0Compaction() {
  std::set<std::string> result;
  
  if (vstorage_->num_levels() <= 1) {
    if (info_log_ != nullptr) {
      ROCKS_LOG_INFO(info_log_, "GetPossibleTargetFilesForL0Compaction: 没有L1层");
    }
    return result;
  }
  
  // 获取L0层的所有文件
  const std::vector<FileMetaData*>& l0_files = vstorage_->LevelFiles(0);
  if (l0_files.empty()) {
    if (info_log_ != nullptr) {
      ROCKS_LOG_INFO(info_log_, "GetPossibleTargetFilesForL0Compaction: L0没有文件");
    }
    return result;
  }
  
  if (info_log_ != nullptr) {
    ROCKS_LOG_INFO(info_log_, "GetPossibleTargetFilesForL0Compaction: L0层有 %zu 个文件", 
                   l0_files.size());
  }
  
  // 计算L0文件的综合键范围
  Slice smallest_key, largest_key;
  bool first_key = true;
  
  for (FileMetaData* f : l0_files) {
    if (first_key) {
      smallest_key = f->smallest.user_key();
      largest_key = f->largest.user_key();
      first_key = false;
    } else {
      if (vstorage_->InternalComparator()->user_comparator()->Compare(
          f->smallest.user_key(), smallest_key) < 0) {
        smallest_key = f->smallest.user_key();
      }
      if (vstorage_->InternalComparator()->user_comparator()->Compare(
          f->largest.user_key(), largest_key) > 0) {
        largest_key = f->largest.user_key();
      }
    }
  }
  
  if (first_key) {
    if (info_log_ != nullptr) {
      ROCKS_LOG_INFO(info_log_, "GetPossibleTargetFilesForL0Compaction: 无法确定L0键范围");
    }
    return result;
  }
  
  if (info_log_ != nullptr) {
    ROCKS_LOG_INFO(info_log_, "L0的综合键范围: [%s, %s]", 
                   ToReadableString(smallest_key).c_str(),
                   ToReadableString(largest_key).c_str());
  }
  
  // 查找与L0键范围重叠的L1文件
  const std::vector<FileMetaData*>& l1_files = vstorage_->LevelFiles(1);
  for (FileMetaData* f : l1_files) {
    // 使用GetRangeOverlap方法或直接检查键范围重叠
    if (vstorage_->InternalComparator()->user_comparator()->Compare(
          f->largest.user_key(), smallest_key) >= 0 &&
        vstorage_->InternalComparator()->user_comparator()->Compare(
          f->smallest.user_key(), largest_key) <= 0) {
      // 文件键范围与L0重叠
      if (info_log_ != nullptr) {
        ROCKS_LOG_INFO(info_log_, "找到与L0重叠的L1文件: %s，键范围: [%s, %s]", 
                       std::to_string(f->fd.GetNumber()).c_str(),
                       ToReadableString(f->smallest.user_key()).c_str(),
                       ToReadableString(f->largest.user_key()).c_str());
      }
      result.insert(std::to_string(f->fd.GetNumber()));
    }
  }
  
  if (info_log_ != nullptr) {
    ROCKS_LOG_INFO(info_log_, "GetPossibleTargetFilesForL0Compaction: 找到 %zu 个L1文件与L0重叠", 
                   result.size());
    if (!result.empty()) {
      std::string files_str;
      for (const auto& file : result) {
        files_str += file + " ";
      }
      ROCKS_LOG_DEBUG(info_log_, "预测的L1文件: %s", files_str.c_str());
    }
  }
  
  return result;
}

// 获取与源层键范围重叠的目标层文件
std::set<std::string> CompactionPredictor::GetTargetLevelFilesForCompaction(
    int source_level, int target_level, const std::set<std::string>& source_files) {
  std::set<std::string> result;
  
  if (source_level < 0 || source_level >= vstorage_->num_levels() - 1 ||
      target_level <= source_level || target_level >= vstorage_->num_levels()) {
    if (info_log_ != nullptr) {
      ROCKS_LOG_WARN(info_log_, "GetTargetLevelFilesForCompaction: 无效的层级 - 源层: %d, 目标层: %d", 
                     source_level, target_level);
    }
    return result;
  }
  
  if (source_files.empty()) {
    if (info_log_ != nullptr) {
      ROCKS_LOG_INFO(info_log_, "GetTargetLevelFilesForCompaction: 没有源文件");
    }
    return result;
  }
  
  if (info_log_ != nullptr) {
    ROCKS_LOG_INFO(info_log_, "GetTargetLevelFilesForCompaction: 源层 %d 有 %zu 个文件，查找与之重叠的目标层 %d 文件", 
                   source_level, source_files.size(), target_level);
  }
  
  // 先找到源文件的键范围
  Slice smallest_key, largest_key;
  bool first_key = true;
  
  // 从源文件名映射到FileMetaData
  for (const auto& source_file_str : source_files) {
    for (FileMetaData* f : vstorage_->LevelFiles(source_level)) {
      if (std::to_string(f->fd.GetNumber()) == source_file_str) {
        if (first_key) {
          smallest_key = f->smallest.user_key();
          largest_key = f->largest.user_key();
          first_key = false;
        } else {
          if (vstorage_->InternalComparator()->user_comparator()->Compare(
              f->smallest.user_key(), smallest_key) < 0) {
            smallest_key = f->smallest.user_key();
          }
          if (vstorage_->InternalComparator()->user_comparator()->Compare(
              f->largest.user_key(), largest_key) > 0) {
            largest_key = f->largest.user_key();
          }
        }
        break;
      }
    }
  }
  
  if (first_key) {
    if (info_log_ != nullptr) {
      ROCKS_LOG_INFO(info_log_, "GetTargetLevelFilesForCompaction: 无法确定源文件的键范围");
    }
    return result;
  }
  
  if (info_log_ != nullptr) {
    ROCKS_LOG_INFO(info_log_, "源文件的综合键范围: [%s, %s]", 
                   ToReadableString(smallest_key).c_str(),
                   ToReadableString(largest_key).c_str());
  }
  
  // 查找与源文件键范围重叠的目标层文件
  const std::vector<FileMetaData*>& target_files = vstorage_->LevelFiles(target_level);
  for (FileMetaData* f : target_files) {
    if (f->being_compacted) continue;
    if (vstorage_->InternalComparator()->user_comparator()->Compare(
          f->largest.user_key(), smallest_key) >= 0 &&
        vstorage_->InternalComparator()->user_comparator()->Compare(
          f->smallest.user_key(), largest_key) <= 0) {
      // 文件键范围与源文件重叠
      if (info_log_ != nullptr) {
        ROCKS_LOG_INFO(info_log_, "找到与源文件重叠的目标层文件: %s，键范围: [%s, %s]", 
                       std::to_string(f->fd.GetNumber()).c_str(),
                       ToReadableString(f->smallest.user_key()).c_str(),
                       ToReadableString(f->largest.user_key()).c_str());
      }
      result.insert(std::to_string(f->fd.GetNumber()));
    }
  }
  
  if (info_log_ != nullptr) {
    ROCKS_LOG_INFO(info_log_, "GetTargetLevelFilesForCompaction: 找到 %zu 个目标层文件与源文件重叠", 
                   result.size());
    if (!result.empty()) {
      std::string files_str;
      for (const auto& file : result) {
        files_str += file + " ";
      }
      ROCKS_LOG_DEBUG(info_log_, "预测的目标层文件: %s", files_str.c_str());
    }
  }
  
  return result;
}

// 检查中间层级的得分，如果中间层的得分都大于0.8，则返回true
bool CompactionPredictor::CheckIntermediateLevelsBetween(int start_level, int target_level) {
  if (start_level >= target_level) {
    if (info_log_ != nullptr) {
      ROCKS_LOG_WARN(info_log_, "CheckIntermediateLevelsBetween: 起始层级 %d >= 目标层级 %d", 
                     start_level, target_level);
    }
    return false;
  }
  
  if (start_level + 1 == target_level) {
    // 没有中间层级
    return true;
  }
  
  if (info_log_ != nullptr) {
    ROCKS_LOG_INFO(info_log_, "CheckIntermediateLevelsBetween: 检查层级 %d 到 %d 之间的中间层", 
                   start_level, target_level);
  }
  
  // 检查所有中间层级的分数是否都 > 0.8
  for (int level = start_level + 1; level < target_level; level++) {
    double score = vstorage_->CompactionScore(level);
    if (score <= 0.8) {
      if (info_log_ != nullptr) {
        ROCKS_LOG_INFO(info_log_, "中间层级 %d 的分数: %.2f <= 0.8，不满足条件", level, score);
      }
      return false;
    }
    
    if (info_log_ != nullptr) {
      ROCKS_LOG_INFO(info_log_, "中间层级 %d 的分数: %.2f > 0.8，满足条件", level, score);
    }
  }
  
  if (info_log_ != nullptr) {
    ROCKS_LOG_INFO(info_log_, "所有中间层级的分数都 > 0.8，条件满足");
  }
  
  return true;
}

std::set<std::string> CompactionPredictor::GetLevelCompactionFiles(int level) {
  std::set<std::string> result;
  if (level < 0 || level >= vstorage_->num_levels() - 1) {
    if (info_log_ != nullptr) {
      ROCKS_LOG_WARN(info_log_, "GetLevelCompactionFiles: 无效的层级 %d", level);
    }
    return result;
  }
  if (info_log_ != nullptr) {
    ROCKS_LOG_INFO(info_log_, "GetLevelCompactionFiles: 层级 %d 总文件数: %zu", 
                   level, vstorage_->LevelFiles(level).size());
  }

  // 新增：RR策略下采用顺序推进+批量无重叠选取
  if (immutable_options_ && mutable_cf_options_ &&
      immutable_options_->compaction_pri == kRoundRobin &&
      immutable_options_->compaction_style == kCompactionStyleLevel) {
    const auto& files = vstorage_->LevelFiles(level);
    int next_index = vstorage_->NextCompactionIndex(level);
    if (next_index < 0 || next_index >= static_cast<int>(files.size())) return result;
    uint64_t total_size = 0;
    uint64_t max_bytes = mutable_cf_options_->max_compaction_bytes;
    for (int i = next_index; i < static_cast<int>(files.size()); ++i) {
      FileMetaData* f = files[i];
      if (f->being_compacted) break;
      if (!result.empty()) {
        // 检查与前一个文件是否有重叠
        auto last_file = files[i-1];
        if (vstorage_->InternalComparator()->user_comparator()->Compare(
              last_file->largest.user_key(), f->smallest.user_key()) >= 0) {
          break; // 有重叠，停止
        }
      }
      if (total_size + f->fd.file_size > max_bytes) break;
      result.insert(std::to_string(f->fd.GetNumber()));
      total_size += f->fd.file_size;
    }
    if (info_log_ != nullptr) {
      ROCKS_LOG_INFO(info_log_, "[RR预测] 层级 %d 预测了 %zu 个文件", level, result.size());
    }
    return result;
  }

  // 原有clean cut递归扩展重叠文件，直到没有新文件加入
  int next_index = vstorage_->NextCompactionIndex(level);
  if (next_index < 0) {
    return result;
  }
  FileMetaData* start_file = vstorage_->LevelFiles(level)[next_index];
  if (info_log_ != nullptr) {
    ROCKS_LOG_INFO(info_log_, "层级 %d 的起始文件: %s，键范围: [%s, %s]", 
                   level, 
                   std::to_string(start_file->fd.GetNumber()).c_str(),
                   ToReadableString(start_file->smallest.user_key()).c_str(),
                   ToReadableString(start_file->largest.user_key()).c_str());
  }
  std::set<FileMetaData*> current_set, last_set;
  current_set.insert(start_file);
  do {
    last_set = current_set;
    Slice min_key, max_key;
    bool first = true;
    for (auto* f : current_set) {
      if (first) {
        min_key = f->smallest.user_key();
        max_key = f->largest.user_key();
        first = false;
      } else {
        if (vstorage_->InternalComparator()->user_comparator()->Compare(f->smallest.user_key(), min_key) < 0) {
          min_key = f->smallest.user_key();
        }
        if (vstorage_->InternalComparator()->user_comparator()->Compare(f->largest.user_key(), max_key) > 0) {
          max_key = f->largest.user_key();
        }
      }
    }
    for (FileMetaData* f : vstorage_->LevelFiles(level)) {
      if (f->being_compacted) continue;
      if (current_set.count(f)) continue;
      if (vstorage_->InternalComparator()->user_comparator()->Compare(f->largest.user_key(), min_key) >= 0 &&
          vstorage_->InternalComparator()->user_comparator()->Compare(f->smallest.user_key(), max_key) <= 0) {
        current_set.insert(f);
        if (info_log_ != nullptr) {
          ROCKS_LOG_INFO(info_log_, "clean cut扩展: 加入重叠文件: %s，键范围: [%s, %s]", 
                         std::to_string(f->fd.GetNumber()).c_str(),
                         ToReadableString(f->smallest.user_key()).c_str(),
                         ToReadableString(f->largest.user_key()).c_str());
        }
      }
    }
  } while (current_set.size() > last_set.size());
  for (auto* f : current_set) {
    result.insert(std::to_string(f->fd.GetNumber()));
  }
  if (info_log_ != nullptr) {
    ROCKS_LOG_INFO(info_log_, "层级 %d clean cut后预测了 %zu 个文件", level, result.size());
    if (!result.empty()) {
      std::string files_str;
      for (const auto& file : result) {
        files_str += file + " ";
      }
      ROCKS_LOG_DEBUG(info_log_, "clean cut预测的文件: %s", files_str.c_str());
    }
  }
  return result;
}

double CompactionPredictor::CalculateNewScore(int level, const std::set<std::string>& files_to_remove) {
  if (level < 0 || level >= vstorage_->num_levels() - 1) {
    if (info_log_ != nullptr) {
      ROCKS_LOG_WARN(info_log_, "CalculateNewScore: 无效的层级 %d", level);
    }
    return 0.0;
  }
  
  if (files_to_remove.empty()) {
    return vstorage_->CompactionScore(level);
  }
  
  // 获取当前层级的分数
  double current_score = vstorage_->CompactionScore(level);
  
  if (info_log_ != nullptr) {
    ROCKS_LOG_INFO(info_log_, "CalculateNewScore: 层级 %d 当前分数: %.2f，预计移除 %zu 个文件", 
                   level, current_score, files_to_remove.size());
  }
  
  // 计算将要移除的文件大小
  uint64_t total_size = 0;
  uint64_t files_to_remove_size = 0;
  
  for (FileMetaData* f : vstorage_->LevelFiles(level)) {
    if (f->being_compacted) continue;
    total_size += f->fd.file_size;
    
    if (files_to_remove.find(std::to_string(f->fd.GetNumber())) != files_to_remove.end()) {
      files_to_remove_size += f->fd.file_size;
      if (info_log_ != nullptr) {
        ROCKS_LOG_DEBUG(info_log_, "将移除文件: %s，大小: %llu", 
                       std::to_string(f->fd.GetNumber()).c_str(),
                       static_cast<unsigned long long>(f->fd.file_size));
      }
    }
  }
  
  if (total_size == 0) {
    if (info_log_ != nullptr) {
      ROCKS_LOG_INFO(info_log_, "CalculateNewScore: 层级 %d 的总文件大小为0", level);
    }
    return current_score;
  }
  
  // 计算要移除的文件所占的百分比
  double removal_ratio = static_cast<double>(files_to_remove_size) / static_cast<double>(total_size);
  
  // 计算新的分数
  double new_score = current_score * (1.0 - removal_ratio);
  
  if (info_log_ != nullptr) {
    ROCKS_LOG_INFO(info_log_, "CalculateNewScore: 层级 %d 移除文件大小: %llu (总大小: %llu，占比: %.2f%%)，预计新分数: %.2f", 
                   level, 
                   static_cast<unsigned long long>(files_to_remove_size),
                   static_cast<unsigned long long>(total_size),
                   removal_ratio * 100.0,
                   new_score);
  }
  
  return new_score;
}

bool CompactionPredictor::KeysInRangeOverlapWithFile(int level, const Slice& smallest_key,
                                          const Slice& largest_key,
                                          const std::string& file_number) {
  const auto& level_files = vstorage_->LevelFiles(level);
  for (const auto& file : level_files) {
    if (std::to_string(file->fd.GetNumber()) == file_number) {
      // 检查键范围是否有重叠
      if (!(smallest_key.compare(file->largest.user_key()) > 0 ||
            largest_key.compare(file->smallest.user_key()) < 0)) {
        return true;
      }
    }
  }
  return false;
}

// 检查两个键范围是否在指定层有顺序相关性
bool CompactionPredictor::Before(int /*level*/, const Slice& /*smallest_key*/,
                                const Slice& largest_key,
                                const Slice& file_smallest, 
                                const Slice& /*file_largest*/) {
  // 如果键范围没有重叠且最大键小于文件的最小键，则说明在该文件之前
  return largest_key.compare(file_smallest) < 0;
}

// 从预测集合中删除已经被compaction的文件
void CompactionPredictor::RemoveCompactedFiles(const std::set<std::string>& compacted_files) {
  for (const auto& file : compacted_files) {
    auto it = predicted_files_.find(file);
    if (it != predicted_files_.end()) {
      ROCKS_LOG_INFO(nullptr, "文件 %s 已被compaction，从预测集合中移除", file.c_str());
      predicted_files_.erase(it);
    }
  }
}

std::set<std::string> CompactionPredictor::GetNextCompactionFilesFrom(
    int level, const std::set<std::string>& excluded_files) {
  std::set<std::string> result;
  
  if (level < 0 || level >= vstorage_->num_levels() - 1) {
    if (info_log_ != nullptr) {
      ROCKS_LOG_WARN(info_log_, "GetNextCompactionFilesFrom: 无效的层级 %d", level);
    }
    return result;
  }
  
  if (info_log_ != nullptr) {
    ROCKS_LOG_INFO(info_log_, "GetNextCompactionFilesFrom: 层级 %d，排除 %zu 个已预测的文件", 
                   level, excluded_files.size());
  }
  
  // L0 层的预测逻辑不同
  if (level == 0) {
    if (info_log_ != nullptr) {
      ROCKS_LOG_WARN(info_log_, "对L0调用GetNextCompactionFilesFrom，L0需要特殊处理");
    }
    return result;
  }
  
  // 找到没有被排除的最大文件
  FileMetaData* largest_file = nullptr;
  for (FileMetaData* f : vstorage_->LevelFiles(level)) {
    if (excluded_files.find(std::to_string(f->fd.GetNumber())) != excluded_files.end()) {
      continue;  // 已被排除
    }
    
    if (f->being_compacted) continue;
    
    if (largest_file == nullptr || f->fd.file_size > largest_file->fd.file_size) {
      largest_file = f;
    }
  }
  
  if (largest_file != nullptr) {
    if (info_log_ != nullptr) {
      ROCKS_LOG_INFO(info_log_, "选择层级 %d 中未被排除的最大文件: %s (大小: %llu)", 
                     level, 
                     std::to_string(largest_file->fd.GetNumber()).c_str(),
                     static_cast<unsigned long long>(largest_file->fd.file_size));
    }
    
    result.insert(std::to_string(largest_file->fd.GetNumber()));
    
    // 查找与最大文件键范围重叠的文件
    for (FileMetaData* f : vstorage_->LevelFiles(level)) {
      if (f->being_compacted) continue;
      if (f->fd.GetNumber() == largest_file->fd.GetNumber() ||
          excluded_files.find(std::to_string(f->fd.GetNumber())) != excluded_files.end()) {
        continue;  // 跳过最大文件本身和已排除的文件
      }
      
      if (vstorage_->InternalComparator()->Compare(f->smallest, largest_file->largest) <= 0 &&
          vstorage_->InternalComparator()->Compare(f->largest, largest_file->smallest) >= 0) {
        // 文件与最大文件重叠
        if (info_log_ != nullptr) {
          ROCKS_LOG_INFO(info_log_, "找到与最大文件重叠的文件: %s，键范围: [%s, %s]", 
                         std::to_string(f->fd.GetNumber()).c_str(),
                         ToReadableString(f->smallest.user_key()).c_str(),
                         ToReadableString(f->largest.user_key()).c_str());
        }
        result.insert(std::to_string(f->fd.GetNumber()));
      }
    }
  } else {
    if (info_log_ != nullptr) {
      ROCKS_LOG_INFO(info_log_, "层级 %d 没有未被排除的文件", level);
    }
  }
  
  if (info_log_ != nullptr) {
    ROCKS_LOG_INFO(info_log_, "GetNextCompactionFilesFrom: 层级 %d 额外预测了 %zu 个文件", 
                   level, result.size());
    if (!result.empty()) {
      std::string files_str;
      for (const auto& file : result) {
        files_str += file + " ";
      }
      ROCKS_LOG_DEBUG(info_log_, "额外预测的文件: %s", files_str.c_str());
    }
  }
  
  return result;
}

void CompactionPredictor::RemoveIncorrectPredictedFiles(
    const std::set<std::string>& incorrect_files) {
  // 从预测集合中删除不正确的文件
  for (const auto& file : incorrect_files) {
    auto it = predicted_files_.find(file);
    if (it != predicted_files_.end()) {
      ROCKS_LOG_INFO(nullptr, "文件 %s 是错误预测，从预测集合中移除", file.c_str());
      predicted_files_.erase(it);
    }
  }
  
  // 简化处理：不需要添加到incorrect_predicted_files_集合中，只需删除即可
  // incorrect_predicted_files_.insert(incorrect_files.begin(), incorrect_files.end());
}

} // namespace ROCKSDB_NAMESPACE