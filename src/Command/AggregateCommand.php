<?php

declare(strict_types=1);

namespace App\Command;

use Flow\ETL\DSL\CSV;
use Flow\ETL\DSL\Json;
use Flow\ETL\DSL\To;
use Flow\ETL\Filesystem\SaveMode;
use Flow\ETL\Flow;
use Flow\ETL\GroupBy\Aggregation;
use Flow\ETL\Window;
use Symfony\Component\Console\Attribute\AsCommand;
use Symfony\Component\Console\Command\Command;
use Symfony\Component\Console\Input\InputArgument;
use Symfony\Component\Console\Input\InputInterface;
use Symfony\Component\Console\Output\OutputInterface;

use function Flow\ETL\DSL\lit;
use function Flow\ETL\DSL\ref;

#[AsCommand('aggregate')]
final class AggregateCommand extends Command
{
    public function __construct()
    {
        parent::__construct();
    }

    protected function configure(): void
    {
        $this->addArgument('org', InputArgument::REQUIRED);
        $this->addArgument('repository', InputArgument::REQUIRED);
    }

    protected function execute(InputInterface $input, OutputInterface $output): int
    {
        $org = $input->getArgument('org');
        $repository = $input->getArgument('repository');

        (new Flow())
            ->read(Json::from(__DIR__."/../../warehouse/dev/{$org}/{$repository}/pr/date_utc=*/*"))

            // Select unique data
            ->withEntry('user', ref('user')->arrayGet('login'))
            ->select('date_utc', 'user')

            // Remove bots from report
            ->filter(ref('user')->notEquals(lit('dependabot[bot]')))

            // Use window function to get details about usage
            ->withEntry('rank', Window::partitionBy(ref('date_utc'), ref('user'))->orderBy(ref('user'))->rank())

            ->withEntry('month_utc', ref('date_utc')->toDateTime('Y-m-d')->dateFormat('Y-m'))

            // Group by date & user
            ->groupBy('month_utc', 'user')
            ->sortBy(ref('month_utc')->desc()->desc())

            // Select top contributors
            ->groupBy('month_utc')->aggregate(Aggregation::first(ref('user')))

            ->write(To::output(false))
            // Remove potential duplicates before saving

            // Save with overwrite
            ->mode(SaveMode::Overwrite)
            ->write(CSV::to(__DIR__."/../../warehouse/dev/{$org}/{$repository}/report/year.csv"))

            // Execute
            ->run()
        ;

        return Command::SUCCESS;
    }
}
