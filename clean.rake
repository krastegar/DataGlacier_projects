Rake::Task['assets:precompile'].enhance do
  FileUtils.remove_dir('node_modules', true)
end
