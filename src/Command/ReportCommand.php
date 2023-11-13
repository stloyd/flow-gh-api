<?php

declare(strict_types=1);

namespace App\Command;

use App\Factory\GithubRequestFactory;
use Flow\ETL\Adapter\Http\PsrHttpClientDynamicExtractor;
use Flow\ETL\ConfigBuilder;
use Flow\ETL\DSL\CSV;
use Flow\ETL\DSL\From;
use Flow\ETL\DSL\Json;
use Flow\ETL\DSL\To;
use Flow\ETL\Filesystem\SaveMode;
use Flow\ETL\Flow;
use Flow\ETL\GroupBy\Aggregation;
use Flow\ETL\Window;
use Http\Client\Curl\Client;
use Nyholm\Psr7\Factory\Psr17Factory;
use Symfony\Component\Console\Attribute\AsCommand;
use Symfony\Component\Console\Command\Command;
use Symfony\Component\Console\Input\InputArgument;
use Symfony\Component\Console\Input\InputInterface;
use Symfony\Component\Console\Output\OutputInterface;
use function Flow\ETL\DSL\not;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\when;

#[AsCommand('report')]
final class ReportCommand extends Command
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

        (new Flow((new ConfigBuilder)->putInputIntoRows()->build()))
            ->read(Json::from(__DIR__."/../../warehouse/dev/{$org}/{$repository}/pr/date_utc=*"))

//            ->limit(100)

            ->withEntry("user", ref("user")->arrayGet("login"))
            ->select('date_utc', 'user')
            ->withEntry('rank', Window::partitionBy(ref("user"))->rank())
//            ->groupBy('date_utc', 'user')->aggregate(Aggregation::sum(ref("rank")))
//            ->sortBy(ref("date_utc")->desc(), ref("rank_sum")->desc())
            ->write(To::output(false))
//            ->partitionBy(ref('date_utc'))
//            ->write(CSV::to(__DIR__."/../../warehouse/dev/{$org}/{$repository}/report/"))

            ->dropDuplicates('date_utc', 'user', "rank")
            // Save with overwrite
            ->mode(SaveMode::Overwrite)
            ->write(CSV::to(__DIR__."/../../warehouse/dev/{$org}/{$repository}/report/year.csv"))
            // Execute
            ->run()
        ;

        return Command::SUCCESS;
    }
}
