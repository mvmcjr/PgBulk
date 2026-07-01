using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Storage.ValueConversion;

namespace PgBulk.Tests;

public class MyContext : DbContext
{
    public MyContext(DbContextOptions options) : base(options)
    {
    }

    public DbSet<TestRow> TestRows => Set<TestRow>();
    public DbSet<ConverterTestRow> ConverterTestRows => Set<ConverterTestRow>();

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.HasPostgresEnum<TestEnum>();
        modelBuilder.Entity<TestRow>(e => { e.Property(i => i.Id).ValueGeneratedNever(); });

        var stringWrapperConverter = new ValueConverter<StringWrapper, string>(
            v => v.Value,
            v => new StringWrapper(v));

        modelBuilder.Entity<ConverterTestRow>(e =>
        {
            e.Property(i => i.Id).ValueGeneratedNever();
            e.Property(i => i.Wrapped).HasConversion(stringWrapperConverter);
        });
    }
}