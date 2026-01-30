#!/usr/bin/env python3
"""
Batch enrich job descriptions with pre-written content.
This script generates professional descriptions based on job titles and companies.
"""

import os
import psycopg2
from psycopg2.extras import RealDictCursor
from dotenv import load_dotenv

load_dotenv()

DB_CONFIG = {
    'dbname': os.environ.get('DB_NAME', 'jobs_comprehensive'),
    'user': os.environ.get('DB_USER', 'noahhopkins'),
    'password': os.environ.get('DB_PASSWORD', ''),
    'host': os.environ.get('DB_HOST', 'localhost'),
    'port': int(os.environ.get('DB_PORT', 5432))
}


def get_db():
    return psycopg2.connect(**DB_CONFIG)


# Role-based description templates
ROLE_DESCRIPTIONS = {
    # Scientific roles
    'biological scientist': """This Biological Scientist position involves conducting research and analysis related to biological systems and environmental science. You will design studies, collect and analyze data, prepare reports, and apply scientific methods to address environmental challenges and regulatory requirements.

Day-to-day work includes fieldwork, laboratory analysis, data interpretation, and collaboration with multidisciplinary teams. You'll ensure compliance with environmental regulations while contributing to sound environmental management practices.

The ideal candidate has a strong foundation in biological sciences, attention to detail, and the ability to communicate technical findings clearly. This role suits someone passionate about environmental stewardship who enjoys applying scientific rigor to real-world problems.""",

    'chemist': """This Chemist position involves conducting chemical analyses, developing methods, and ensuring quality control for various materials and processes. You will design experiments, operate laboratory equipment, interpret results, and prepare technical reports.

Day-to-day work includes sample analysis, method validation, documentation, and collaboration with engineers and scientists. You'll apply chemistry expertise to solve practical problems while maintaining safety and quality standards.

The ideal candidate has strong analytical skills, laboratory experience, and attention to detail. This role suits someone who enjoys hands-on scientific work and takes pride in producing accurate, reliable results.""",

    'physicist': """This Physicist position applies physics principles to solve complex technical problems. You will conduct research, develop models, analyze data, and contribute to projects requiring physics expertise.

Day-to-day work includes theoretical analysis, experimental design, data interpretation, and technical writing. You'll collaborate with engineers and other scientists to address challenging technical questions.

The ideal candidate has strong analytical and mathematical skills with the ability to apply physics concepts to practical applications. This role suits someone who enjoys tackling complex problems at the intersection of theory and application.""",

    'psychologist': """This Psychologist position provides psychological services including assessment, treatment, and consultation. You will conduct evaluations, develop treatment plans, provide therapy, and collaborate with interdisciplinary teams to support client well-being.

Day-to-day work includes clinical interviews, psychological testing, individual and group therapy, documentation, and case consultation. You'll apply evidence-based practices while maintaining ethical standards.

The ideal candidate has doctoral-level training in psychology, strong clinical skills, and genuine commitment to helping others. This role suits someone who values both the science and art of psychological practice.""",

    'environmental scientist': """This Environmental Scientist position focuses on assessing and protecting environmental quality. You will conduct environmental assessments, monitor conditions, ensure regulatory compliance, and develop recommendations for environmental management.

Day-to-day work includes site assessments, data collection and analysis, report writing, and collaboration with engineers and regulators. You'll apply scientific expertise to protect human health and the environment.

The ideal candidate has strong analytical skills, knowledge of environmental regulations, and the ability to communicate technical information clearly. This role suits someone passionate about environmental protection and sustainability.""",

    # Government/Federal roles
    'contract specialist': """This Contract Specialist position involves managing the full lifecycle of government contracts, from pre-award planning through contract closeout. Day-to-day responsibilities include reviewing procurement requests, developing solicitation documents, evaluating contractor proposals, and ensuring compliance with federal acquisition regulations.

The successful candidate will negotiate contract terms, manage modifications, and serve as the primary point of contact between the government and contractors. This role requires strong analytical skills to evaluate cost proposals and technical approaches, as well as excellent communication abilities to work with program managers and legal counsel.

Ideal candidates have experience with federal contracting processes and enjoy working in a structured environment where attention to detail and regulatory compliance are paramount. This is an excellent opportunity to develop expertise in government procurement while supporting critical agency missions.""",

    'correctional officer': """This Correctional Officer position maintains security and order within a correctional facility. You will supervise inmates, enforce rules and regulations, respond to emergencies, and contribute to rehabilitation efforts while ensuring safety for staff, inmates, and the public.

Day-to-day responsibilities include conducting counts, searches, and inspections; monitoring inmate activities; preparing incident reports; and responding to disturbances. You'll maintain professional boundaries while treating all individuals with dignity.

The ideal candidate has strong situational awareness, physical fitness, and the ability to remain calm under pressure. This role suits someone with a commitment to public safety who can balance security requirements with humane treatment of incarcerated individuals.""",

    'computer scientist': """As a Computer Scientist in this role, you will design, develop, and implement software solutions that support mission-critical operations. This position involves analyzing complex problems, developing algorithms, and creating software systems that meet both technical requirements and user needs.

Day-to-day work includes writing and reviewing code, participating in system design discussions, conducting technical research, and collaborating with cross-functional teams. You'll evaluate emerging technologies and recommend solutions that improve operational efficiency and capability.

The ideal candidate combines strong programming skills with the ability to think creatively about complex technical challenges. This role suits someone who enjoys both hands-on coding and higher-level system architecture, and who thrives in an environment where their work directly impacts organizational effectiveness.""",

    'data scientist': """This Data Scientist role focuses on extracting actionable insights from complex datasets to inform strategic decisions. You will apply statistical methods, machine learning algorithms, and data visualization techniques to solve real-world problems and improve operational outcomes.

Responsibilities include developing predictive models, conducting exploratory data analysis, and presenting findings to both technical and non-technical stakeholders. You'll work closely with subject matter experts to understand business problems and translate them into data-driven solutions.

The successful candidate has strong foundations in statistics and programming, combined with intellectual curiosity and the ability to communicate complex findings clearly. This position offers the opportunity to work on challenging problems with meaningful impact.""",

    'engineer': """This Engineering position involves designing, developing, and maintaining systems that meet operational requirements and technical specifications. You will apply engineering principles to solve complex problems, conduct analyses, and ensure solutions meet quality and safety standards.

Day-to-day responsibilities include technical design work, testing and validation, documentation, and collaboration with multidisciplinary teams. You'll participate in project planning, provide technical expertise to stakeholders, and contribute to continuous improvement initiatives.

The ideal candidate has strong analytical and problem-solving skills, attention to detail, and the ability to work both independently and as part of a team. This role offers opportunities to work on challenging technical problems while developing expertise in your engineering discipline.""",

    'physician': """This Physician position provides comprehensive medical care to patients, combining clinical expertise with a patient-centered approach. Responsibilities include conducting examinations, diagnosing conditions, developing treatment plans, and coordinating care with other healthcare providers.

The role involves managing a patient panel, documenting clinical encounters, participating in quality improvement initiatives, and staying current with medical advances. You'll work as part of an interdisciplinary healthcare team committed to delivering high-quality, evidence-based care.

The ideal candidate is a board-certified physician with strong clinical skills and genuine compassion for patients. This position offers the opportunity to practice medicine in a supportive environment with competitive compensation and comprehensive benefits.""",

    'nurse': """This Nursing position provides direct patient care and serves as a key member of the healthcare team. Responsibilities include assessing patient conditions, administering medications, coordinating care plans, and educating patients and families about health management.

Day-to-day work involves monitoring patient status, documenting care activities, collaborating with physicians and other providers, and ensuring adherence to clinical protocols. You'll advocate for patients while maintaining a safe and therapeutic environment.

The ideal candidate has current nursing licensure, strong clinical skills, and genuine dedication to patient welfare. This role suits someone who thrives in a dynamic healthcare environment and values both autonomy and teamwork in their practice.""",

    'accountant': """This Accountant position manages financial records, ensures compliance with regulations, and provides analysis to support organizational decision-making. Responsibilities include preparing financial statements, reconciling accounts, processing transactions, and maintaining accurate documentation.

The role involves analyzing financial data, identifying discrepancies, preparing reports for management, and supporting audit activities. You'll work with various stakeholders to ensure financial operations run smoothly and in accordance with applicable standards.

The ideal candidate has strong attention to detail, proficiency with accounting software, and the ability to explain financial information clearly. This position offers the opportunity to develop expertise while contributing to sound financial management.""",

    'analyst': """This Analyst position involves gathering, analyzing, and interpreting data to support organizational objectives. You will develop reports, identify trends, and provide recommendations based on thorough analysis of available information.

Day-to-day responsibilities include collecting and validating data, conducting quantitative and qualitative analyses, preparing presentations, and collaborating with stakeholders to understand their information needs. You'll translate complex findings into actionable insights.

The ideal candidate has strong analytical skills, proficiency with data tools, and the ability to communicate findings effectively to diverse audiences. This role suits someone who enjoys solving problems through data and continuous learning.""",

    'specialist': """This Specialist position applies subject matter expertise to support organizational objectives and solve complex challenges. You will serve as a resource for your functional area, providing guidance, developing solutions, and ensuring quality outcomes.

Responsibilities include analyzing situations, developing recommendations, implementing solutions, and documenting processes. You'll collaborate with team members and stakeholders to achieve goals while maintaining high professional standards.

The ideal candidate brings relevant expertise combined with strong problem-solving and communication skills. This role offers the opportunity to deepen your specialization while making meaningful contributions to the organization's mission.""",

    'manager': """This management position leads a team in achieving organizational objectives while developing talent and fostering a positive work environment. Responsibilities include setting priorities, allocating resources, monitoring performance, and ensuring deliverables meet quality standards.

Day-to-day work involves coaching team members, facilitating communication, solving problems, and representing the team to leadership. You'll balance operational demands with strategic planning and continuous improvement initiatives.

The ideal candidate has demonstrated leadership ability, strong communication skills, and the capacity to motivate others toward shared goals. This role suits someone who finds satisfaction in developing people and building high-performing teams.""",

    'inspector': """This Inspector position ensures compliance with regulations, standards, and safety requirements through systematic examination and evaluation. You will conduct inspections, document findings, and work with stakeholders to address any identified issues.

Responsibilities include planning inspection activities, gathering evidence, preparing reports, and providing guidance on compliance requirements. You'll maintain current knowledge of applicable regulations and contribute to maintaining high safety and quality standards.

The ideal candidate has strong attention to detail, technical knowledge relevant to the inspection domain, and the ability to communicate findings diplomatically yet firmly. This role suits someone committed to upholding standards and ensuring public safety.""",

    'officer': """This Officer position involves maintaining security, enforcing regulations, and ensuring the safety of personnel and facilities. Responsibilities include monitoring activities, responding to incidents, preparing reports, and following established protocols.

Day-to-day work includes patrol duties, access control, emergency response, and coordination with other security personnel. You'll maintain vigilance while providing professional service to all individuals you encounter.

The ideal candidate has strong situational awareness, physical fitness, and the ability to remain calm under pressure. This role suits someone with a commitment to protecting others and maintaining a safe environment.""",

    'assistant': """This Assistant position provides essential support to ensure smooth operations and efficient workflow. Responsibilities include administrative tasks, coordination activities, documentation, and serving as a point of contact for various stakeholders.

Day-to-day work involves managing schedules, preparing materials, tracking information, and facilitating communication. You'll anticipate needs and proactively address issues to support team effectiveness.

The ideal candidate has strong organizational skills, attention to detail, and the ability to manage multiple priorities. This role suits someone who takes pride in enabling others to succeed through excellent support.""",

    'technician': """This Technician position involves hands-on work maintaining, troubleshooting, and operating specialized equipment or systems. You will apply technical knowledge to ensure proper functioning and address issues as they arise.

Responsibilities include performing maintenance procedures, diagnosing problems, making repairs, and documenting activities. You'll follow safety protocols and contribute to operational efficiency through your technical expertise.

The ideal candidate has relevant technical training, problem-solving ability, and manual dexterity. This role suits someone who enjoys working with their hands and takes satisfaction in keeping systems running smoothly.""",

    'worker': """This position involves performing essential operational tasks that contribute to organizational effectiveness. You will carry out assigned duties while maintaining quality standards and following safety procedures.

Day-to-day work includes completing tasks as directed, maintaining work areas, following protocols, and communicating with supervisors about progress and any issues. You'll contribute to team goals through reliable performance.

This role suits someone who takes pride in doing quality work and values being part of a team. The position offers opportunities to develop skills and potentially advance within the organization.""",

    'coordinator': """This Coordinator position manages logistics and communications to ensure activities run smoothly. You will organize schedules, track progress, facilitate information flow, and solve problems as they arise.

Responsibilities include planning activities, maintaining records, communicating with stakeholders, and ensuring resources are available when needed. You'll serve as a central point of contact and help keep projects on track.

The ideal candidate has strong organizational and interpersonal skills, with the ability to manage multiple priorities simultaneously. This role suits someone who excels at bringing order to complexity and enjoys facilitating collaboration.""",

    'software engineer': """This Software Engineer role involves designing, building, and maintaining software applications that solve real business problems. You will write clean, efficient code, participate in code reviews, and collaborate with product and design teams to deliver features users love.

Day-to-day work includes implementing new features, debugging issues, improving system performance, and contributing to technical architecture decisions. You'll work in an agile environment with regular releases and continuous learning opportunities.

The ideal candidate has strong programming skills, experience with modern development practices, and a passion for creating quality software. This role suits someone who enjoys both the technical challenges of coding and the satisfaction of building products that make a difference.""",

    'product manager': """This Product Manager role drives the strategy and execution for a product or feature area. You will define product vision, prioritize the roadmap, and work closely with engineering, design, and stakeholders to deliver value to users.

Responsibilities include gathering customer insights, writing requirements, making trade-off decisions, and measuring success through data. You'll balance user needs with business objectives while navigating technical constraints.

The ideal candidate has strong analytical and communication skills, user empathy, and the ability to influence without authority. This role suits someone who enjoys wearing multiple hats and taking ownership of outcomes.""",

    'sales': """This Sales position focuses on building relationships with prospects and customers to drive revenue growth. You will identify opportunities, conduct outreach, present solutions, and close deals while maintaining high standards of customer service.

Day-to-day work includes prospecting, qualifying leads, conducting demonstrations, negotiating terms, and collaborating with internal teams to ensure customer success. You'll manage a pipeline and work toward quota achievement.

The ideal candidate has strong communication skills, resilience, and genuine curiosity about customer problems. This role suits someone motivated by achievement who enjoys the challenge of consultative selling.""",

    'marketing': """This Marketing position develops and executes campaigns that build brand awareness and generate demand. You will create content, manage channels, analyze performance, and collaborate with teams across the organization.

Responsibilities include planning campaigns, creating materials, optimizing based on data, and staying current with marketing trends and best practices. You'll contribute creative ideas while executing with precision.

The ideal candidate combines creativity with analytical thinking and strong communication skills. This role suits someone who enjoys the variety of marketing work and takes satisfaction in driving measurable results.""",

    'hr': """This Human Resources position supports the employee lifecycle from recruitment through offboarding. You will partner with managers on talent needs, handle employee relations matters, and ensure HR programs run effectively.

Day-to-day work includes recruiting, onboarding, policy administration, benefits questions, and contributing to HR initiatives. You'll balance employee advocacy with organizational needs while maintaining confidentiality.

The ideal candidate has strong interpersonal skills, attention to detail, and genuine interest in helping people succeed at work. This role suits someone who values both the strategic and service aspects of HR.""",

    'finance': """This Finance position manages financial processes and provides analysis to support business decisions. You will prepare reports, analyze variances, support budgeting processes, and ensure accuracy in financial records.

Responsibilities include financial reporting, forecasting, ad-hoc analysis, and collaboration with business partners on financial matters. You'll contribute to financial discipline while supporting operational needs.

The ideal candidate has strong analytical skills, attention to detail, and the ability to explain financial concepts clearly. This role suits someone who enjoys working with numbers and providing insights that inform decisions.""",

    'customer support': """This Customer Support position helps customers succeed with products and services through excellent service and problem-solving. You will respond to inquiries, troubleshoot issues, and ensure positive customer experiences.

Day-to-day work includes handling tickets, phone calls, or chats, documenting solutions, escalating complex issues, and providing feedback to product teams. You'll balance efficiency with empathy in every interaction.

The ideal candidate has strong communication skills, patience, and genuine desire to help others. This role suits someone who finds satisfaction in solving problems and turning frustrated customers into advocates.""",

    'operations': """This Operations position ensures smooth day-to-day functioning of business processes. You will manage workflows, solve problems, track metrics, and continuously improve how things get done.

Responsibilities include process management, coordination across teams, troubleshooting issues, and implementing improvements. You'll balance immediate operational needs with longer-term optimization efforts.

The ideal candidate has strong organizational and problem-solving skills with attention to detail. This role suits someone who enjoys making things work better and takes pride in operational excellence.""",

    'design': """This Design position creates user experiences that are both beautiful and functional. You will conduct research, develop concepts, create designs, and collaborate with product and engineering to bring ideas to life.

Day-to-day work includes user research, wireframing, prototyping, visual design, and iterating based on feedback. You'll advocate for users while navigating business and technical constraints.

The ideal candidate has strong design skills, user empathy, and the ability to communicate design decisions effectively. This role suits someone who is passionate about creating experiences that delight users.""",
}


def generate_description(title, company, location, salary_range=None, existing_desc=None):
    """Generate a professional job description based on title and company."""
    title_lower = title.lower()

    # Priority order for matching - more specific matches first
    priority_matches = [
        # Exact/specific matches first
        ('biological scientist', 'biological scientist'),
        ('environmental scientist', 'environmental scientist'),
        ('physical scientist', 'environmental scientist'),  # map to environmental
        ('computer scientist', 'computer scientist'),
        ('data scientist', 'data scientist'),
        ('contract specialist', 'contract specialist'),
        ('correctional officer', 'correctional officer'),
        ('police officer', 'officer'),
        ('security guard', 'officer'),
        ('software engineer', 'software engineer'),
        ('product manager', 'product manager'),
        ('practical nurse', 'nurse'),
        ('nursing assistant', 'assistant'),
        ('dental assistant', 'assistant'),
        ('food service worker', 'worker'),
        ('sales store checker', 'worker'),
        ('meatcutting worker', 'worker'),
        ('laborer', 'worker'),
        ('heavy mobile equipment repairer', 'technician'),
        ('pipefitter', 'technician'),
        ('aviation safety inspector', 'inspector'),
        ('criminal investigator', 'inspector'),
        # General matches
        ('chemist', 'chemist'),
        ('physicist', 'physicist'),
        ('psychologist', 'psychologist'),
        ('physician', 'physician'),
        ('medical officer', 'physician'),
        ('flight surgeon', 'physician'),
        ('nurse', 'nurse'),
        ('engineer', 'engineer'),
        ('accountant', 'accountant'),
        ('budget analyst', 'analyst'),
        ('cost analyst', 'analyst'),
        ('analyst', 'analyst'),
        ('inspector', 'inspector'),
        ('officer', 'officer'),
        ('manager', 'manager'),
        ('director', 'manager'),
        ('supervisor', 'manager'),
        ('specialist', 'specialist'),
        ('coordinator', 'coordinator'),
        ('secretary', 'assistant'),
        ('assistant', 'assistant'),
        ('technician', 'technician'),
        ('repairer', 'technician'),
        ('sales', 'sales'),
        ('marketing', 'marketing'),
        ('hr', 'hr'),
        ('human resources', 'hr'),
        ('finance', 'finance'),
        ('support', 'customer support'),
        ('customer service', 'customer support'),
        ('operations', 'operations'),
        ('design', 'design'),
        ('worker', 'worker'),
    ]

    # Find the best matching template
    best_match = None
    for search_term, template_key in priority_matches:
        if search_term in title_lower:
            best_match = template_key
            break

    if best_match:
        base_desc = ROLE_DESCRIPTIONS[best_match]
    else:
        # Generic fallback
        base_desc = f"""This {title} position at {company} offers an opportunity to contribute your skills and expertise to meaningful work. You will collaborate with colleagues to achieve organizational objectives while developing professionally.

Day-to-day responsibilities include performing job-specific duties, collaborating with team members, maintaining quality standards, and contributing to continuous improvement. You'll have opportunities to learn and grow while making a real impact.

The ideal candidate brings relevant skills and experience along with strong work ethic and collaborative spirit. This role suits someone who takes initiative and values both individual contribution and teamwork."""

    # Customize with company and location
    if company and 'Veterans Health Administration' in company:
        base_desc = base_desc.replace(
            'The ideal candidate',
            f'Working for the Veterans Health Administration in {location}, you\'ll serve those who served our country. The ideal candidate'
        )
    elif company and ('Air Force' in company or 'Department of the Air Force' in company):
        base_desc = base_desc.replace(
            'The ideal candidate',
            f'As part of the Air Force civilian workforce in {location}, you\'ll support critical defense missions. The ideal candidate'
        )
    elif company and 'Bureau of Prisons' in company:
        base_desc = base_desc.replace(
            'The ideal candidate',
            f'Working for the Federal Bureau of Prisons in {location}, you\'ll contribute to public safety and inmate rehabilitation. The ideal candidate'
        )
    elif company and 'Federal Aviation Administration' in company:
        base_desc = base_desc.replace(
            'The ideal candidate',
            f'At the FAA in {location}, you\'ll help maintain the safety of the national airspace system. The ideal candidate'
        )
    elif company and ('Indian Health Service' in company or 'IHS' in company):
        base_desc = base_desc.replace(
            'The ideal candidate',
            f'Working for the Indian Health Service in {location}, you\'ll provide essential healthcare to American Indian and Alaska Native communities. The ideal candidate'
        )
    elif company and 'National Park Service' in company:
        base_desc = base_desc.replace(
            'The ideal candidate',
            f'At the National Park Service in {location}, you\'ll help preserve America\'s natural and cultural heritage. The ideal candidate'
        )

    return base_desc


def enrich_batch(limit=100, dry_run=False):
    """Enrich a batch of jobs with short/missing descriptions."""
    conn = get_db()
    enriched = 0

    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute("""
            SELECT id, title, company_name, location, description, salary_range
            FROM watchable_positions
            WHERE (description IS NULL OR LENGTH(description) < 300)
              AND title IS NOT NULL AND company_name IS NOT NULL
            ORDER BY
              CASE WHEN salary_range IS NOT NULL THEN 0 ELSE 1 END,
              LENGTH(COALESCE(description, ''))
            LIMIT %s
        """, (limit,))

        jobs = cur.fetchall()
        print(f"Found {len(jobs)} jobs to enrich")

        for job in jobs:
            new_desc = generate_description(
                job['title'],
                job['company_name'],
                job['location'],
                job['salary_range'],
                job['description']
            )

            if new_desc and len(new_desc) > len(job['description'] or ''):
                if dry_run:
                    print(f"\n{'='*60}")
                    print(f"WOULD ENRICH: {job['title']} at {job['company_name']}")
                    print(f"Location: {job['location']}")
                    print(f"Salary: {job['salary_range']}")
                    print(f"\nNew description ({len(new_desc)} chars):")
                    print(new_desc[:500] + "..." if len(new_desc) > 500 else new_desc)
                else:
                    cur.execute("""
                        UPDATE watchable_positions
                        SET description = %s
                        WHERE id = %s
                    """, (new_desc, job['id']))
                    enriched += 1

                    if enriched % 100 == 0:
                        conn.commit()
                        print(f"  Enriched {enriched} jobs...")

        if not dry_run:
            conn.commit()
            print(f"\nEnriched {enriched} job descriptions")

    conn.close()
    return enriched


if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser(description='Batch enrich job descriptions')
    parser.add_argument('--limit', type=int, default=100, help='Number of jobs to process')
    parser.add_argument('--dry-run', action='store_true', help='Preview without saving')
    parser.add_argument('--all', action='store_true', help='Process all jobs needing enrichment')

    args = parser.parse_args()

    if args.all:
        # Process in batches until done
        total = 0
        while True:
            enriched = enrich_batch(limit=500, dry_run=args.dry_run)
            total += enriched
            if enriched == 0:
                break
            print(f"Total enriched so far: {total}")
        print(f"\nCompleted! Total enriched: {total}")
    else:
        enrich_batch(limit=args.limit, dry_run=args.dry_run)
