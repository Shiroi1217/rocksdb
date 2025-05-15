// db/compaction/compaction_predictor.cc
#include "db/compaction/compaction_predictor.h"
#include "logging/logging.h"

namespace ROCKSDB_NAMESPACE {

// 由于构造函数在头文件中已定义为内联函数，这里删除构造函数定义
// CompactionPredictor::CompactionPredictor(const VersionStorageInfo* vstorage)
//     : vstorage_(vstorage) {}

std::set<std::string> CompactionPredictor::PredictCompactionFiles() {
  // 清空predicted_files_集合，存储本次预测结果
  std::set<std::string> current_predicted;
  
  // 确保我们获取到最新的层级信息和score值
  ROCKS_LOG_INFO(nullptr, "开始预测下一轮compaction文件，确保获取最新层级信息...");
  
  // 手动获取最新的score值，因为VersionStorageInfo的score可能是过时的
  // 注意：由于 VersionStorageInfo 是 const 的，我们不能调用 ComputeCompactionScore
  // 只能使用已经计算好的 score 值
  if (immutable_options_ && mutable_cf_options_) {
    ROCKS_LOG_INFO(nullptr, "使用当前已有的compaction score值");
    
    // 输出每一层的score值进行调试验证
    for (int level = 0; level < vstorage_->num_levels() - 1; level++) {
      double score = vstorage_->CompactionScore(level);
      int compaction_level = vstorage_->CompactionScoreLevel(0);
      ROCKS_LOG_INFO(nullptr, "Level %d 的compaction score: %.2f (最高优先级的level是: %d)",
                   level, score, compaction_level);
    }
  } else {
    ROCKS_LOG_WARN(nullptr, "无法更新compaction score值，因为没有提供必要的options");
  }
  
  // 记录每个层级的score，找出score最高的层级
  int highest_score_level = -1;
  double highest_score = 0.0;
  bool has_level_above_one = false;
  
  for (int level = 0; level < vstorage_->num_levels() - 1; level++) {
    // 获取最新的compaction score
    double score = vstorage_->CompactionScore(level);
    ROCKS_LOG_INFO(nullptr, "Level %d 的compaction score: %.2f", level, score);
    
    // 记录得分最高的层级
    if (score > highest_score) {
      highest_score = score;
      highest_score_level = level;
    }
    
    // 记录是否有层级的score > 1.0
    if (score > 1.0) {
      has_level_above_one = true;
    }
  }
  
  // 首先检查L1到L2的特殊情况
  bool l1_to_l2_special_case = CheckL1ToL2Compaction();
  
  // 首先处理score > 1.0的层级
  if (has_level_above_one) {
    // 优先处理L0层，因为L0到L1的compaction通常优先级更高
    if (vstorage_->CompactionScore(0) > 1.0) {
      ROCKS_LOG_INFO(nullptr, "L0 score > 1.0，优先预测L0到L1的compaction");
      // 对于L0到L1的compaction，我们跳过预测L0层的文件，只预测L1层的文件
      auto l1_files = GetPossibleTargetFilesForL0Compaction();
      
      if (!l1_files.empty()) {
        ROCKS_LOG_INFO(nullptr, "预测到L0到L1 compaction可能会影响 %zu 个L1文件", 
                      l1_files.size());
        
        // 将所有L1文件添加到预测集合中
        current_predicted.insert(l1_files.begin(), l1_files.end());
      } else {
        ROCKS_LOG_INFO(nullptr, "未找到可能受L0 compaction影响的L1文件");
      }
    }
    
    // 处理其他score > 1.0的层级（L1及以上）
    for (int level = 1; level < vstorage_->num_levels() - 1; level++) {
      double score = vstorage_->CompactionScore(level);
      if (score <= 1.0) continue;
      
      ROCKS_LOG_INFO(nullptr, "Level %d 的score > 1.0，开始预测", level);
      
      // 获取该层级可能进行compaction的文件
      auto level_files = GetLevelCompactionFiles(level);
      
      if (level_files.empty()) {
        ROCKS_LOG_INFO(nullptr, "Level %d 未找到适合compaction的文件", level);
        continue;
      }
      
      ROCKS_LOG_INFO(nullptr, "Level %d 预测到 %zu 个文件将进行compaction", 
                    level, level_files.size());
      
      // 将该层级的文件添加到当前预测集合中
      current_predicted.insert(level_files.begin(), level_files.end());
      
      // 计算新的score并检查是否需要继续compaction
      double new_score = CalculateNewScore(level, level_files);
      if (new_score > 1.0) {
        // The score is still > 1.0 after removing the files that are already
        // predicted to be compacted. We need to predict more files.
        
        // 尝试找其他可能的起点进行预测
        std::set<std::string> already_predicted = level_files;
        
        ROCKS_LOG_INFO(nullptr, "Level %d 在移除预测文件后Score仍为 %.2f，继续预测", 
                      level, new_score);
        
        // 最多重复三次
        for (int i = 0; i < 3 && new_score > 1.0; ++i) {
          // 寻找未被预测过的、最适合作为起点的文件
          auto additional_files = GetNextCompactionFilesFrom(level, already_predicted);
          if (additional_files.empty()) {
            ROCKS_LOG_INFO(nullptr, "Level %d 未找到更多适合预测的文件，停止预测", level);
            break;  // 没有更多适合的文件了
          }
          
          ROCKS_LOG_INFO(nullptr, "Level %d 第%d轮额外预测到 %zu 个文件", 
                        level, i+1, additional_files.size());
          
          current_predicted.insert(additional_files.begin(), additional_files.end());
          for (const auto& file : additional_files) {
            already_predicted.insert(file);
          }
          
          new_score = CalculateNewScore(level, already_predicted);
          ROCKS_LOG_INFO(nullptr, "Level %d 第%d轮预测后Score为 %.2f", 
                        level, i+1, new_score);
        }
      }
      
      // 如果当前层有预测，也需要检查下一层的文件
      if (!level_files.empty() && level + 1 < vstorage_->num_levels()) {
        auto target_level_files = GetTargetLevelFilesForCompaction(level, level + 1, level_files);
        
        if (!target_level_files.empty()) {
          ROCKS_LOG_INFO(nullptr, "预测到Level %d 到 Level %d compaction会涉及 %zu 个Level %d文件", 
                       level, level + 1, target_level_files.size(), level + 1);
          
          // 直接添加到预测集合中
          current_predicted.insert(target_level_files.begin(), target_level_files.end());
        }
      }
    }
  } else if (l1_to_l2_special_case) {
    // L1到L2的特殊情况：尽管L1的score < 1.0，但由于L0积压等原因，可能会触发L1到L2的compaction
    ROCKS_LOG_INFO(nullptr, "检测到L1到L2的特殊compaction情况，尝试预测L1到L2的文件");
    
    // 获取L1层可能进行compaction的文件
    auto l1_files = GetLevelCompactionFiles(1);
    if (!l1_files.empty()) {
      ROCKS_LOG_INFO(nullptr, "L1到L2特殊情况：预测到 %zu 个L1文件将进行compaction", 
                    l1_files.size());
      
      // 将L1文件添加到预测集合中
      current_predicted.insert(l1_files.begin(), l1_files.end());
      
      // 检查L2层可能受影响的文件
      auto l2_files = GetTargetLevelFilesForCompaction(1, 2, l1_files);
      if (!l2_files.empty()) {
        ROCKS_LOG_INFO(nullptr, "预测到L1到L2 compaction会涉及 %zu 个L2文件", 
                       l2_files.size());
        
        // 添加L2文件到预测集合中
        current_predicted.insert(l2_files.begin(), l2_files.end());
      }
    } else {
      ROCKS_LOG_INFO(nullptr, "L1到L2特殊情况：未找到适合compaction的L1文件");
    }
  } else if (highest_score_level >= 0) {
    // 如果没有层级score > 1.0且不是L1到L2的特殊情况，则预测score最高的层级
    int level = highest_score_level;
    ROCKS_LOG_INFO(nullptr, "没有层级score > 1.0，选择score最高的Level %d (score: %.2f)进行预测", 
                  level, highest_score);
    
    // 获取该层级可能进行compaction的文件
    auto level_files = GetLevelCompactionFiles(level);
    
    if (!level_files.empty()) {
      ROCKS_LOG_INFO(nullptr, "Level %d 预测到 %zu 个文件将进行compaction", 
                    level, level_files.size());
      
      // 将该层级的文件添加到当前预测集合中
      current_predicted.insert(level_files.begin(), level_files.end());
      
      // 如果当前层有预测，也需要检查下一层的文件
      if (level + 1 < vstorage_->num_levels()) {
        auto target_level_files = GetTargetLevelFilesForCompaction(level, level + 1, level_files);
        
        if (!target_level_files.empty()) {
          ROCKS_LOG_INFO(nullptr, "预测到Level %d 到 Level %d compaction会涉及 %zu 个Level %d文件", 
                       level, level + 1, target_level_files.size(), level + 1);
          
          // 直接添加到预测集合中
          current_predicted.insert(target_level_files.begin(), target_level_files.end());
        }
      }
    } else {
      ROCKS_LOG_INFO(nullptr, "Level %d 未找到适合compaction的文件", level);
    }
  }
  
  // 更新预测次数
  for (const auto& file : current_predicted) {
    predicted_files_[file]++;
    ROCKS_LOG_INFO(nullptr, "文件 %s 被预测次数: %d", 
                  file.c_str(), predicted_files_[file]);
  }
  
  if (current_predicted.empty()) {
    ROCKS_LOG_INFO(nullptr, "未能预测到下一轮compaction的文件");
  } else {
    ROCKS_LOG_INFO(nullptr, "本轮预测完成，共预测到 %zu 个文件", current_predicted.size());
  }
  return current_predicted;
}

// 获取L0到L1 compaction可能涉及的L1文件
std::set<std::string> CompactionPredictor::GetPossibleTargetFilesForL0Compaction() {
  std::set<std::string> l1_files;
  
  // 没有L0文件或L1文件，不需要预测
  if (vstorage_->NumLevelFiles(0) == 0 || vstorage_->NumLevelFiles(1) == 0) {
    return l1_files;
  }
  
  // 获取L0层所有文件的键范围
  const auto& l0_files = vstorage_->LevelFiles(0);
  const auto& l1_files_vec = vstorage_->LevelFiles(1);
  
  // 先确定L0文件的整体键范围
  InternalKey smallest_l0, largest_l0;
  bool first = true;
  
  for (const auto& file : l0_files) {
    if (file->being_compacted) {
      continue;  // 跳过正在compaction的文件
    }
    
    if (first) {
      smallest_l0 = file->smallest;
      largest_l0 = file->largest;
      first = false;
    } else {
      if (smallest_l0.user_key().compare(file->smallest.user_key()) > 0) {
        smallest_l0 = file->smallest;
      }
      if (largest_l0.user_key().compare(file->largest.user_key()) < 0) {
        largest_l0 = file->largest;
      }
    }
  }
  
  // 如果找不到有效的L0文件范围，返回空集合
  if (first) {
    return l1_files;
  }
  
  ROCKS_LOG_INFO(nullptr, "L0文件整体键范围: [%s, %s]", 
                smallest_l0.DebugString(true).c_str(),
                largest_l0.DebugString(true).c_str());
  
  // 寻找与L0文件键范围重叠的L1文件
  for (const auto& file : l1_files_vec) {
    if (file->being_compacted) {
      continue;  // 跳过正在compaction的文件
    }
    
    // 检查是否与L0整体范围重叠
    if (!(file->largest.user_key().compare(smallest_l0.user_key()) < 0 ||
          file->smallest.user_key().compare(largest_l0.user_key()) > 0)) {
      std::string file_number = std::to_string(file->fd.GetNumber());
      l1_files.insert(file_number);
      
      ROCKS_LOG_INFO(nullptr, "L1文件 %s 与L0键范围重叠，添加到预测集合，键范围: [%s, %s]",
                    file_number.c_str(),
                    file->smallest.DebugString(true).c_str(),
                    file->largest.DebugString(true).c_str());
    }
  }
  
  return l1_files;
}

// 获取与源层键范围重叠的目标层文件
std::set<std::string> CompactionPredictor::GetTargetLevelFilesForCompaction(
    int source_level, int target_level, const std::set<std::string>& source_files) {
  std::set<std::string> target_files;
  
  if (source_files.empty() || target_level >= vstorage_->num_levels()) {
    return target_files;
  }
  
  // 获取源层文件的键范围
  std::vector<FileMetaData*> source_file_meta;
  const auto& source_level_files = vstorage_->LevelFiles(source_level);
  
  for (const auto& file : source_level_files) {
    std::string file_number = std::to_string(file->fd.GetNumber());
    if (source_files.find(file_number) != source_files.end()) {
      source_file_meta.push_back(file);
    }
  }
  
  if (source_file_meta.empty()) {
    return target_files;
  }
  
  // 确定源文件的整体键范围
  InternalKey smallest_source = source_file_meta[0]->smallest;
  InternalKey largest_source = source_file_meta[0]->largest;
  
  for (size_t i = 1; i < source_file_meta.size(); i++) {
    if (smallest_source.user_key().compare(source_file_meta[i]->smallest.user_key()) > 0) {
      smallest_source = source_file_meta[i]->smallest;
    }
    if (largest_source.user_key().compare(source_file_meta[i]->largest.user_key()) < 0) {
      largest_source = source_file_meta[i]->largest;
    }
  }
  
  ROCKS_LOG_INFO(nullptr, "Level %d 源文件整体键范围: [%s, %s]", 
                source_level,
                smallest_source.DebugString(true).c_str(),
                largest_source.DebugString(true).c_str());
  
  // 寻找与源层文件键范围重叠的目标层文件
  const auto& target_level_files = vstorage_->LevelFiles(target_level);
  
  for (const auto& file : target_level_files) {
    if (file->being_compacted) {
      continue;  // 跳过正在compaction的文件
    }
    
    // 检查是否与源层整体范围重叠
    if (!(file->largest.user_key().compare(smallest_source.user_key()) < 0 ||
          file->smallest.user_key().compare(largest_source.user_key()) > 0)) {
      std::string file_number = std::to_string(file->fd.GetNumber());
      target_files.insert(file_number);
      
      ROCKS_LOG_INFO(nullptr, "Level %d 文件 %s 与源层键范围重叠，添加到预测集合，键范围: [%s, %s]",
                    target_level, file_number.c_str(),
                    file->smallest.DebugString(true).c_str(),
                    file->largest.DebugString(true).c_str());
    }
  }
  
  return target_files;
}

// 检查中间层级的得分，如果中间层的得分都大于0.8，则返回true
bool CompactionPredictor::CheckIntermediateLevelsBetween(int start_level, int target_level) {
  for (int level = start_level - 1; level > target_level; --level) {
    if (vstorage_->CompactionScore(level) <= 0.8) {
      return false;
    }
  }
  return true;
}

std::set<std::string> CompactionPredictor::GetLevelCompactionFiles(int level) {
  std::set<std::string> files;
  const auto& level_files = vstorage_->LevelFiles(level);
  
  if (level_files.empty()) {
    return files;
  }
  
  // 使用不同的选择逻辑，取决于level
  if (level == 0) {
    // 对于L0，尝试选择最老的文件作为起点(通常是文件编号最小的)
    size_t oldest_index = 0;
    uint64_t oldest_number = std::numeric_limits<uint64_t>::max();
    
    for (size_t i = 0; i < level_files.size(); i++) {
      if (!level_files[i]->being_compacted && 
          level_files[i]->fd.GetNumber() < oldest_number) {
        oldest_number = level_files[i]->fd.GetNumber();
        oldest_index = i;
      }
    }
    
    // 如果找到了起始文件
    if (oldest_number != std::numeric_limits<uint64_t>::max()) {
      const FileMetaData* start_file = level_files[oldest_index];
      std::string start_file_number = std::to_string(start_file->fd.GetNumber());
      files.insert(start_file_number);
      
      ROCKS_LOG_INFO(nullptr, "L0层选择最老的文件 %s 作为预测起点", 
                    start_file_number.c_str());
      
      // 对于L0，找到键范围重叠的文件
      InternalKey smallest = start_file->smallest;
      InternalKey largest = start_file->largest;
      
      for (size_t i = 0; i < level_files.size(); i++) {
        if (i == oldest_index) continue;  // 跳过起点文件自身
        
        const FileMetaData* other_file = level_files[i];
        if (other_file->being_compacted) continue;
        
        // 检查文件是否与起始文件有重叠
        if (!(other_file->largest.user_key().compare(smallest.user_key()) < 0 ||
              other_file->smallest.user_key().compare(largest.user_key()) > 0)) {
          std::string file_number = std::to_string(other_file->fd.GetNumber());
          files.insert(file_number);
          
          // 更新键范围
          if (other_file->smallest.user_key().compare(smallest.user_key()) < 0) {
            smallest = other_file->smallest;
          }
          if (other_file->largest.user_key().compare(largest.user_key()) > 0) {
            largest = other_file->largest;
          }
        }
      }
    }
  } else {
    // 对于非L0层级，使用compaction_pri来选择文件
    const std::vector<int>& file_scores = vstorage_->FilesByCompactionPri(level);
    if (file_scores.empty()) {
      ROCKS_LOG_INFO(nullptr, "Level %d 没有按压缩优先级排序的文件", level);
      return files;
    }
    
    // 根据CompactionPicker的逻辑选择起始文件
    unsigned int cmp_idx = vstorage_->NextCompactionIndex(level);
    ROCKS_LOG_INFO(nullptr, "Level %d 的当前NextCompactionIndex: %u", level, cmp_idx);
    
    // 如果已经到达文件列表末尾，重置到开始位置
    if (cmp_idx >= file_scores.size()) {
      cmp_idx = 0;
    }
    
    // 寻找合适的起始文件
    bool found_start_file = false;
    size_t checked_files = 0;
    while (checked_files < file_scores.size() && !found_start_file) {
      if (cmp_idx >= file_scores.size()) {
        cmp_idx = 0;
      }
      
      int index = file_scores[cmp_idx];
      if (index >= static_cast<int>(level_files.size())) {
        cmp_idx = (cmp_idx + 1) % file_scores.size();
        checked_files++;
        continue;
      }
      
      auto* f = level_files[index];
      
      if (f->being_compacted) {
        cmp_idx = (cmp_idx + 1) % file_scores.size();
        checked_files++;
        continue;
      }
      
      // 找到了起始文件
      found_start_file = true;
      
      // 使用找到的起点文件
      std::string start_file_number = std::to_string(f->fd.GetNumber());
      files.insert(start_file_number);
      
      ROCKS_LOG_INFO(nullptr, "Level %d 选择文件 %s (#%d) 作为预测起点，键范围: [%s, %s], 文件大小: %llu bytes", 
                    level, start_file_number.c_str(), index,
                    f->smallest.DebugString(true).c_str(),
                    f->largest.DebugString(true).c_str(),
                    static_cast<unsigned long long>(f->fd.GetFileSize()));
      
      // 执行ExpandInputsToCleanCut操作 - 确保有明确的边界
      InternalKey smallest = f->smallest;
      InternalKey largest = f->largest;
      
      // 寻找clean cut边界内的所有文件
      std::set<size_t> included_indices;
      included_indices.insert(index);
      
      // 模拟ExpandInputsToCleanCut的do-while循环
      size_t old_size;
      do {
        old_size = included_indices.size();
        
        // 找到所有与当前键范围重叠的文件
        for (size_t i = 0; i < level_files.size(); i++) {
          if (included_indices.find(i) != included_indices.end()) {
            continue;  // 已经包含了该文件
          }
          
          const FileMetaData* other_file = level_files[i];
          if (other_file->being_compacted) {
            continue;
          }
          
          // 检查是否与键范围重叠
          if (!(other_file->largest.user_key().compare(smallest.user_key()) < 0 ||
                other_file->smallest.user_key().compare(largest.user_key()) > 0)) {
            // 文件有重叠，添加到集合中
            std::string other_file_number = std::to_string(other_file->fd.GetNumber());
            files.insert(other_file_number);
            included_indices.insert(i);
            
            // 更新键范围
            if (other_file->smallest.user_key().compare(smallest.user_key()) < 0) {
              smallest = other_file->smallest;
            }
            if (other_file->largest.user_key().compare(largest.user_key()) > 0) {
              largest = other_file->largest;
            }
            
            ROCKS_LOG_INFO(nullptr, "Level %d 文件 %s 与键范围重叠，添加到预测集合，扩展键范围至 [%s, %s]", 
                          level, other_file_number.c_str(),
                          smallest.DebugString(true).c_str(),
                          largest.DebugString(true).c_str());
          }
        }
      } while (included_indices.size() > old_size);
      
      // 找到下层与该文件键范围重叠的文件
      if (level + 1 < vstorage_->num_levels()) {
        // 使用扩展后的键范围查找下一层重叠文件
        const auto& next_level_files = vstorage_->LevelFiles(level + 1);
        ROCKS_LOG_INFO(nullptr, "检查Level %d 的 %zu 个文件是否与键范围 [%s, %s] 重叠", 
                      level + 1, next_level_files.size(),
                      smallest.DebugString(true).c_str(),
                      largest.DebugString(true).c_str());
        
        int overlap_count = 0;
        for (const auto& file : next_level_files) {
          // 如果该文件正在被压缩，跳过
          if (file->being_compacted) {
            continue;
          }
          
          // 检查文件是否与键范围重叠
          if (!(file->largest.user_key().compare(smallest.user_key()) < 0 ||
                file->smallest.user_key().compare(largest.user_key()) > 0)) {
            // 文件与键范围重叠，添加到集合中
            std::string file_number = std::to_string(file->fd.GetNumber());
            files.insert(file_number);
            overlap_count++;
            ROCKS_LOG_INFO(nullptr, "Level %d 文件 %s 与上层键范围重叠，添加到预测集合, 键范围: [%s, %s]", 
                          level + 1, file_number.c_str(),
                          file->smallest.DebugString(true).c_str(),
                          file->largest.DebugString(true).c_str());
          }
        }
        
        if (overlap_count == 0) {
          ROCKS_LOG_INFO(nullptr, "Level %d 未找到与上层键范围重叠的文件", level + 1);
        }
      }
      
      // 如果我们只需要一个文件，这里可以打破循环
      if (found_start_file && level > 0) {
        break;
      }
      
      cmp_idx = (cmp_idx + 1) % file_scores.size();
      checked_files++;
    }
    
    // 如果正常流程没有找到文件，尝试基于文件大小选择
    if (files.empty()) {
      // 尝试选择大小最大的文件，这通常是优先考虑的文件
      FileMetaData* largest_file = nullptr;
      uint64_t largest_size = 0;
      for (const auto& f : level_files) {
        if (f->being_compacted) continue;
        
        if (f->fd.GetFileSize() > largest_size) {
          largest_size = f->fd.GetFileSize();
          largest_file = f;
        }
      }
      
      if (largest_file != nullptr) {
        std::string file_number = std::to_string(largest_file->fd.GetNumber());
        files.insert(file_number);
        ROCKS_LOG_INFO(nullptr, "Level %d 基于文件大小选择最大文件 %s (大小: %llu bytes)作为预测起点", 
                      level, file_number.c_str(), 
                      static_cast<unsigned long long>(largest_size));
      }
    }
  }
  
  ROCKS_LOG_INFO(nullptr, "Level %d 总共预测到 %zu 个文件参与compaction", 
                level, files.size());
  return files;
}

double CompactionPredictor::CalculateNewScore(int level, const std::set<std::string>& files_to_remove) {
  if (level < 1 || level >= vstorage_->num_levels()) {
    return 0.0;
  }
  
  // 不计算0层的score值，L0层用文件数量直接判断
  // 对于其他层级，score是基于文件大小计算的
  uint64_t level_size = vstorage_->NumLevelBytes(level);
  uint64_t removed_size = 0;
  
  // 计算要移除的文件的总大小
  const auto& level_files = vstorage_->LevelFiles(level);
  for (const auto& file : level_files) {
    if (files_to_remove.find(std::to_string(file->fd.GetNumber())) != files_to_remove.end()) {
      removed_size += file->fd.GetFileSize();
    }
  }
  
  // 如果移除的文件大小超过了层级大小，说明有错误
  if (removed_size > level_size) {
    return 0.0;
  }
  
  // 计算移除文件后的层级大小
  uint64_t new_level_size = level_size - removed_size;
  
  // 计算当前层级得分与大小的比率，以便计算新的得分
  double score_per_byte = 0.0;
  if (level_size > 0) {
    score_per_byte = vstorage_->CompactionScore(level) / static_cast<double>(level_size);
  }
  
  // 使用新的大小计算得分
  double current_score = score_per_byte * static_cast<double>(new_level_size);
  
  return current_score;
}

// 检查是否可能发生L1到L2的compaction，尽管L1的score < 1.0
bool CompactionPredictor::CheckL1ToL2Compaction() {
  // RocksDB中即使score < 1.0，也可能因为特殊情况进行compaction
  // 特别是当L0有积压，但L0->L1 compaction可能会导致L1过大时
  
  // 检查L0积压情况
  double l0_score = vstorage_->CompactionScore(0);
  if (l0_score <= 1.0) {
    return false;  // L0没有积压
  }
  
  // 检查L1的情况
  double l1_score = vstorage_->CompactionScore(1);
  if (l1_score >= 1.0) {
    return false;  // L1已经需要compaction了，不需要特殊处理
  }
  
  // 检查L1和L2的大小比例
  uint64_t l1_size = vstorage_->NumLevelBytes(1);
  uint64_t l2_size = vstorage_->NumLevelBytes(2);
  
  // 如果L1已经比较大，接近其目标大小的70%以上
  if (l1_score >= 0.7) {
    ROCKS_LOG_INFO(nullptr, "L1 score (%.2f) 接近1.0且L0有积压(score: %.2f)，可能发生L1->L2 compaction",
                  l1_score, l0_score);
    return true;
  }
  
  // 如果L1文件比较多
  if (vstorage_->NumLevelFiles(1) >= 8) {
    ROCKS_LOG_INFO(nullptr, "L1有较多文件(%d)且L0有积压(score: %.2f)，可能发生L1->L2 compaction",
                  vstorage_->NumLevelFiles(1), l0_score);
    return true;
  }
  
  // 如果L1远大于L2，也可能触发compaction
  if (l2_size > 0 && l1_size > 2 * l2_size) {
    ROCKS_LOG_INFO(nullptr, "L1大小(%llu)远大于L2(%llu)，可能发生L1->L2 compaction",
                 static_cast<unsigned long long>(l1_size), 
                 static_cast<unsigned long long>(l2_size));
    return true;
  }
  
  return false;
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
  std::set<std::string> files;
  const auto& level_files = vstorage_->LevelFiles(level);
  
  if (level_files.empty()) {
    return files;
  }
  
  // 寻找未被排除的文件作为新的起点
  const FileMetaData* start_file = nullptr;
  for (const auto& file : level_files) {
    std::string file_number = std::to_string(file->fd.GetNumber());
    if (excluded_files.find(file_number) == excluded_files.end()) {
      start_file = file;
      files.insert(file_number);
      break;
    }
  }
  
  if (start_file == nullptr) {
    // 没有找到合适的起点文件
    return files;
  }
  
  // 找到所有在相同层里键范围重叠的文件
  for (const auto& file : level_files) {
    if (file == start_file) continue;  // 跳过起点文件自身
    
    std::string file_number = std::to_string(file->fd.GetNumber());
    if (excluded_files.find(file_number) != excluded_files.end()) {
      continue;  // 已被排除，跳过
    }
    
    // 检查文件与起点文件是否有重叠
    if (start_file->smallest.user_key().compare(file->largest.user_key()) > 0 ||
        start_file->largest.user_key().compare(file->smallest.user_key()) < 0) {
      continue;  // 文件没有重叠，跳过
    }
    
    // 文件有重叠，添加到集合中
    files.insert(file_number);
  }
  
  // 找到下层与该文件键范围重叠的文件
  if (level + 1 < vstorage_->num_levels()) {
    const Slice& smallest_key = start_file->smallest.user_key();
    const Slice& largest_key = start_file->largest.user_key();
    
    // 检查下层是否有重叠的文件
    const auto& next_level_files = vstorage_->LevelFiles(level + 1);
    for (const auto& file : next_level_files) {
      std::string file_number = std::to_string(file->fd.GetNumber());
      if (excluded_files.find(file_number) != excluded_files.end()) {
        continue;  // 已被排除，跳过
      }
      
      // 检查文件是否与键范围重叠
      const Slice& file_smallest = file->smallest.user_key();
      const Slice& file_largest = file->largest.user_key();
      
      if (!(smallest_key.compare(file_largest) > 0 ||
            largest_key.compare(file_smallest) < 0)) {
        // 文件与键范围重叠，添加到集合中
        files.insert(file_number);
      }
    }
  }
  
  return files;
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