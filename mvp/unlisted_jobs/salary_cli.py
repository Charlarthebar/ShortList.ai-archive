#!/usr/bin/env python3
"""
Salary Intelligence CLI
=======================

Interactive command-line tool for querying salary data.

Usage:
    python salary_cli.py                  # Interactive mode
    python salary_cli.py "Software Engineer"  # Quick lookup
    python salary_cli.py --report "Data Scientist"  # Full report

Author: ShortList.ai
Date: 2026-01-14
"""

import os
import sys
import argparse

os.environ['DB_USER'] = 'noahhopkins'

from salary_analysis import SalaryAnalyzer


def print_quick_stats(analyzer: SalaryAnalyzer, title: str):
    """Print quick salary stats for a title."""
    stats = analyzer.salary_for_title(title)

    if not stats:
        print(f"❌ Insufficient data for '{title}' (need 10+ samples)")
        print("\nTry a broader search term or check spelling.")
        return

    print(f"""
╔══════════════════════════════════════════════════════════════╗
║  {title.upper():^58}  ║
╠══════════════════════════════════════════════════════════════╣
║  Median Salary:     ${stats.median_salary:>12,.0f}                        ║
║  Average Salary:    ${stats.avg_salary:>12,.0f}                        ║
║  Salary Range:      ${stats.min_salary:>12,.0f} - ${stats.max_salary:<12,.0f}        ║
║  25th-75th %ile:    ${stats.p25_salary:>12,.0f} - ${stats.p75_salary:<12,.0f}        ║
║  Sample Size:       {stats.count:>12,} jobs                       ║
╚══════════════════════════════════════════════════════════════╝
""")


def interactive_mode(analyzer: SalaryAnalyzer):
    """Run interactive query mode."""
    print("""
╔══════════════════════════════════════════════════════════════╗
║           SALARY INTELLIGENCE SYSTEM                        ║
║           5.86 Million Jobs Database                        ║
╠══════════════════════════════════════════════════════════════╣
║  Commands:                                                   ║
║    <job title>     - Quick salary lookup                    ║
║    report <title>  - Full report with comparisons           ║
║    states <title>  - Compare salaries by state              ║
║    senior <title>  - Seniority progression                  ║
║    employers <title> - Top paying companies                 ║
║    similar <title> - Find related job titles                ║
║    quit            - Exit                                    ║
╚══════════════════════════════════════════════════════════════╝
""")

    while True:
        try:
            query = input("\n🔍 Enter job title: ").strip()
        except (EOFError, KeyboardInterrupt):
            print("\nGoodbye!")
            break

        if not query:
            continue

        if query.lower() in ['quit', 'exit', 'q']:
            print("Goodbye!")
            break

        # Parse command
        parts = query.split(' ', 1)
        command = parts[0].lower()

        if command == 'report' and len(parts) > 1:
            analyzer.full_report(parts[1])
        elif command == 'states' and len(parts) > 1:
            analyzer.state_comparison(parts[1])
        elif command in ['senior', 'seniority'] and len(parts) > 1:
            analyzer.seniority_progression(parts[1])
        elif command == 'employers' and len(parts) > 1:
            analyzer.employer_comparison(parts[1])
        elif command == 'similar' and len(parts) > 1:
            similar = analyzer.similar_titles(parts[1], limit=20)
            if similar:
                print(f"\nRelated titles for '{parts[1]}':")
                print("-" * 60)
                for s in similar:
                    print(f"  {s['title']:<40} {s['count']:>6,} jobs  ${s['avg_salary']:>10,.0f}")
            else:
                print(f"No similar titles found for '{parts[1]}'")
        else:
            # Default: quick lookup
            print_quick_stats(analyzer, query)


def main():
    parser = argparse.ArgumentParser(
        description='Salary Intelligence CLI - Query 5.86M jobs',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python salary_cli.py "Software Engineer"      Quick lookup
  python salary_cli.py --report "Data Scientist"    Full report
  python salary_cli.py --states "Nurse"         State comparison
  python salary_cli.py                          Interactive mode
        """
    )
    parser.add_argument('title', nargs='?', help='Job title to search')
    parser.add_argument('--report', '-r', metavar='TITLE', help='Generate full report')
    parser.add_argument('--states', '-s', metavar='TITLE', help='Compare by state')
    parser.add_argument('--seniority', metavar='TITLE', help='Seniority progression')
    parser.add_argument('--employers', '-e', metavar='TITLE', help='Top employers')

    args = parser.parse_args()

    # Suppress database logging
    import logging
    logging.getLogger().setLevel(logging.WARNING)

    analyzer = SalaryAnalyzer()

    if args.report:
        analyzer.full_report(args.report)
    elif args.states:
        analyzer.state_comparison(args.states)
    elif args.seniority:
        analyzer.seniority_progression(args.seniority)
    elif args.employers:
        analyzer.employer_comparison(args.employers)
    elif args.title:
        print_quick_stats(analyzer, args.title)
    else:
        interactive_mode(analyzer)


if __name__ == "__main__":
    main()
