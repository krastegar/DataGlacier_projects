Rake::Task['assets:clean'].enhance do
  FileUtils.remove_dir('node_modules', true)
end
