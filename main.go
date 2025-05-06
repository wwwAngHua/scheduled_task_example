package main

import (
	"fmt"
	"log"
	"time"

	"github.com/robfig/cron/v3"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
)

// Task 数据库任务模型
type Task struct {
	ID      uint   `gorm:"primaryKey"`
	Name    string `gorm:"not null"`
	Program string `gorm:"not null"`
	Cron    string `gorm:"not null"`
}

// CronScheduler 封装 cron 调度器和任务映射
type CronScheduler struct {
	cron    *cron.Cron
	taskIDs map[uint]cron.EntryID // 映射数据库任务 ID 到 cron 任务 ID
	db      *gorm.DB
}

// NewCronScheduler 初始化调度器
func NewCronScheduler(db *gorm.DB) *CronScheduler {
	// 设置上海时区
	loc, err := time.LoadLocation("Asia/Shanghai")
	if err != nil {
		log.Fatalf("加载上海时区失败: %v", err)
	}

	return &CronScheduler{
		cron:    cron.New(cron.WithLocation(loc), cron.WithSeconds()),
		taskIDs: make(map[uint]cron.EntryID),
		db:      db,
	}
}

// StartAllTasks 启动数据库中的所有任务
func (cs *CronScheduler) StartAllTasks() error {
	var tasks []Task
	// 查询所有任务
	if err := cs.db.Find(&tasks).Error; err != nil {
		return fmt.Errorf("查询任务失败: %v", err)
	}

	for _, task := range tasks {
		// 添加任务到调度器
		entryID, err := cs.cron.AddFunc(task.Cron, func() {
			fmt.Printf("执行任务 %s (%d): %s\n", task.Name, task.ID, task.Program)
		})
		if err != nil {
			log.Printf("添加任务 %s (%d) 失败: %v", task.Name, task.ID, err)
			continue
		}

		// 记录任务 ID 映射
		cs.taskIDs[task.ID] = entryID
		log.Printf("任务 %s (%d) 已启动，cron: %s", task.Name, task.ID, task.Cron)
	}

	// 启动调度器
	cs.cron.Start()
	return nil
}

// AddTask 添加新任务
func (cs *CronScheduler) AddTask(name, program, cronExpr string) error {
	// 创建任务记录
	task := Task{
		Name:    name,
		Program: program,
		Cron:    cronExpr,
	}
	if err := cs.db.Create(&task).Error; err != nil {
		return fmt.Errorf("创建任务失败: %v", err)
	}

	// 添加到调度器
	entryID, err := cs.cron.AddFunc(cronExpr, func() {
		fmt.Printf("执行任务 %s (%d): %s\n", task.Name, task.ID, task.Program)
	})
	if err != nil {
		// 如果添加失败，删除数据库记录
		cs.db.Delete(&task)
		return fmt.Errorf("添加任务到调度器失败: %v", err)
	}

	// 记录任务 ID 映射
	cs.taskIDs[task.ID] = entryID
	log.Printf("任务 %s (%d) 已添加，cron: %s", task.Name, task.ID, cronExpr)
	return nil
}

// RemoveTask 删除任务
func (cs *CronScheduler) RemoveTask(taskID uint) error {
	// 检查任务是否存在
	var task Task
	if err := cs.db.First(&task, taskID).Error; err != nil {
		return fmt.Errorf("任务 %d 不存在: %v", taskID, err)
	}

	// 从调度器中移除
	if entryID, exists := cs.taskIDs[taskID]; exists {
		cs.cron.Remove(entryID)
		delete(cs.taskIDs, taskID)
	}

	// 从数据库中删除
	if err := cs.db.Delete(&Task{}, taskID).Error; err != nil {
		return fmt.Errorf("删除任务 %d 失败: %v", taskID, err)
	}

	log.Printf("任务 %s (%d) 已删除", task.Name, taskID)
	return nil
}

// 初始化数据库
func initDB() *gorm.DB {
	// MySQL 连接配置（根据实际环境调整）
	dsn := "root:123456@tcp(127.0.0.1:3306)/task?charset=utf8mb4&parseTime=True&loc=Local"
	db, err := gorm.Open(mysql.Open(dsn), &gorm.Config{})
	if err != nil {
		log.Fatalf("连接数据库失败: %v", err)
	}

	// 自动迁移数据库表
	if err := db.AutoMigrate(&Task{}); err != nil {
		log.Fatalf("迁移数据库表失败: %v", err)
	}

	// 插入示例数据
	tasks := []Task{
		{Name: "DailyBackup", Program: "运行数据库备份脚本", Cron: "0 0 0 * * *"},     // 每天午夜
		{Name: "HourlyCheck", Program: "检查系统状态", Cron: "0 0 * * * *"},        // 每小时
		{Name: "BiMinuteReport", Program: "生成每两分钟报告", Cron: "0 */2 * * * *"}, // 每两分钟
	}
	for _, task := range tasks {
		// 仅插入不存在的任务
		var count int64
		db.Model(&Task{}).Where("name = ?", task.Name).Count(&count)
		if count == 0 {
			if err := db.Create(&task).Error; err != nil {
				log.Printf("插入示例任务 %s 失败: %v", task.Name, err)
			} else {
				log.Printf("插入示例任务 %s 成功", task.Name)
			}
		}
	}

	return db
}

func main() {
	// 初始化数据库
	db := initDB()

	// 创建调度器
	scheduler := NewCronScheduler(db)

	// 启动所有任务
	if err := scheduler.StartAllTasks(); err != nil {
		log.Fatalf("启动任务失败: %v", err)
	}

	// 示例：添加和删除任务
	go func() {
		// 等待几秒以观察初始任务
		time.Sleep(10 * time.Second)

		// 添加新任务
		err := scheduler.AddTask("TestTask", "运行测试程序", "0 * * * * *") // 每分钟
		if err != nil {
			log.Printf("添加任务失败: %v", err)
		}

		// 再等待几秒
		time.Sleep(10 * time.Second)

		// 删除任务（假设 ID 为 1）
		err = scheduler.RemoveTask(1)
		if err != nil {
			log.Printf("删除任务失败: %v", err)
		}
	}()

	// 保持程序运行
	select {}
}
